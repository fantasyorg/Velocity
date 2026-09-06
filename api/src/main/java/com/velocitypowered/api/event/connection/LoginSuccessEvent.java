/*
 * Copyright (C) 2018-2021 Velocity Contributors
 *
 * The Velocity API is licensed under the terms of the MIT License. For more details,
 * reference the LICENSE file in the api top-level directory.
 */

package com.velocitypowered.api.event.connection;

import com.google.common.base.Preconditions;
import com.velocitypowered.api.event.annotation.AwaitingEvent;
import com.velocitypowered.api.proxy.Player;
import java.util.UUID;

/**
 * This event is fired right before the proxy tells the client that the login succeeded, and
 * lets a plugin choose the unique id the <em>client</em> is told. It only changes what goes
 * on the wire to the client: the player keeps {@link Player#getUniqueId()} everywhere else on
 * the proxy and towards backend servers. Velocity waits for this event to finish firing before
 * sending the packet, so keep the work in it small.
 */
@AwaitingEvent
public final class LoginSuccessEvent {

  private final Player player;
  private UUID uuid;

  public LoginSuccessEvent(Player player, UUID uuid) {
    this.player = Preconditions.checkNotNull(player, "player");
    this.uuid = Preconditions.checkNotNull(uuid, "uuid");
  }

  public Player getPlayer() {
    return player;
  }

  /**
   * Returns the unique id the client will be told. Defaults to {@link Player#getUniqueId()}.
   *
   * @return the unique id sent to the client
   */
  public UUID getUuid() {
    return uuid;
  }

  /**
   * Sets the unique id the client will be told. The player's id on the proxy is not affected.
   *
   * @param uuid the unique id to send to the client
   */
  public void setUuid(UUID uuid) {
    this.uuid = Preconditions.checkNotNull(uuid, "uuid");
  }

  @Override
  public String toString() {
    return "LoginSuccessEvent{"
        + "player=" + player
        + ", uuid=" + uuid
        + '}';
  }
}
