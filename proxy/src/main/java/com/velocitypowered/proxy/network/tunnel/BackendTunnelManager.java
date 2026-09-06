package com.velocitypowered.proxy.network.tunnel;

import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.config.VelocityConfiguration;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** One {@link BackendTunnel} per backend server that is configured to use the tunnel. */
public final class BackendTunnelManager {

  private final VelocityServer server;
  private final Map<String, BackendTunnel> tunnels = new ConcurrentHashMap<>();

  public BackendTunnelManager(VelocityServer server) {
    this.server = server;
  }

  /** The tunnel for this server, or empty when the server connects the ordinary way. */
  public Optional<BackendTunnel> tunnelFor(RegisteredServer registeredServer) {
    VelocityConfiguration configuration = this.server.getConfiguration();
    if (!configuration.isBackendTunnel()) {
      return Optional.empty();
    }

    String name = registeredServer.getServerInfo().getName();
    List<String> allowed = configuration.getBackendTunnelServers();
    if (!allowed.isEmpty() && !allowed.contains(name)) {
      return Optional.empty();
    }

    InetSocketAddress address = registeredServer.getServerInfo().getAddress();
    return Optional.of(this.tunnels.computeIfAbsent(name, key -> new BackendTunnel(this.server, key, address,
        new InetSocketAddress(address.getHostString(), address.getPort() + configuration.getBackendTunnelPortOffset()))));
  }
}
