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

  /** The server is gone; its tunnel goes with it, and whoever registers the name again starts fresh. */
  public void serverUnregistered(String name) {
    BackendTunnel tunnel = this.tunnels.remove(name);
    if (tunnel != null) {
      tunnel.close();
    }
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

    // Servers are registered on the fly and come back from a restart under the same name with a new
    // port, so a cached tunnel is only reused while it still points at the server's current address.
    InetSocketAddress address = registeredServer.getServerInfo().getAddress();
    return Optional.of(this.tunnels.compute(name, (key, existing) -> {
      if (existing != null && existing.getBackendAddress().equals(address)) {
        return existing;
      }
      if (existing != null) {
        existing.close();
      }
      return new BackendTunnel(this.server, key, address,
          new InetSocketAddress(address.getHostString(), address.getPort() + configuration.getBackendTunnelPortOffset()));
    }));
  }
}
