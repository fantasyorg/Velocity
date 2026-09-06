package com.velocitypowered.proxy.network.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * First handler of a tunnel socket on the proxy: sends the handshake as soon as the socket is up,
 * waits for the backend's answer and swaps itself for the frame codec and the multiplexer.
 */
final class TunnelClientHandshakeHandler extends ByteToMessageDecoder {

  private final byte[] secret;
  private final Supplier<TunnelClientMultiplexer> multiplexerFactory;
  private final CompletableFuture<TunnelClientMultiplexer> ready;

  TunnelClientHandshakeHandler(byte[] secret, Supplier<TunnelClientMultiplexer> multiplexerFactory, CompletableFuture<TunnelClientMultiplexer> ready) {
    this.secret = secret;
    this.multiplexerFactory = multiplexerFactory;
    this.ready = ready;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    ctx.writeAndFlush(TunnelProtocol.encodeHandshake(ctx.alloc(), this.secret));
    ctx.fireChannelActive();
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    if (in.readableBytes() < 6) {
      return;
    }

    int magic = in.readInt();
    byte version = in.readByte();
    byte status = in.readByte();

    if (magic != TunnelProtocol.MAGIC || version != TunnelProtocol.VERSION) {
      fail(ctx, new IOException("Backend " + ctx.channel().remoteAddress() + " did not answer a tunnel handshake"));
      return;
    }
    if (status != TunnelProtocol.STATUS_OK) {
      fail(ctx, new IOException("Backend " + ctx.channel().remoteAddress() + " rejected the tunnel (status " + status + "); check the forwarding secret"));
      return;
    }

    TunnelClientMultiplexer multiplexer = this.multiplexerFactory.get();
    ctx.pipeline().addLast("tunnel-decoder", new TunnelFrameCodec.Decoder());
    ctx.pipeline().addLast("tunnel-encoder", new TunnelFrameCodec.Encoder());
    ctx.pipeline().addLast("tunnel-multiplexer", multiplexer);
    ctx.pipeline().remove(this);
    this.ready.complete(multiplexer);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    this.ready.completeExceptionally(new IOException("Tunnel to " + ctx.channel().remoteAddress() + " closed during the handshake"));
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    fail(ctx, cause);
  }

  private void fail(ChannelHandlerContext ctx, Throwable cause) {
    this.ready.completeExceptionally(cause);
    ctx.close();
  }
}
