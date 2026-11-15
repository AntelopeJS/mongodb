import { AsyncLocalStorage } from "node:async_hooks";
import diagnosticsChannel from "node:diagnostics_channel";
import net from "node:net";

import { isDocument } from "./implementations/database/utils";

const SOCKET_CHANNEL = "net.client.socket";

const OWNER = new AsyncLocalStorage<OwnedSockets>();

/**
 * The client sockets, TLS included, that the work run through {@link OwnedSockets.run} opens,
 * timers and the driver's monitors it starts included. A `MongoClient` cancels a monitor's
 * connection only while its TCP connect is pending, so these are what `shut` destroys to close
 * every connection at once.
 */
export class OwnedSockets {
  private readonly sockets = new Set<net.Socket>();

  private readonly track: diagnosticsChannel.ChannelListener = (message) => {
    const socket: unknown = isDocument(message) ? message.socket : undefined;
    if (socket instanceof net.Socket && OWNER.getStore() === this) {
      this.sockets.add(socket);
    }
  };

  constructor() {
    diagnosticsChannel.subscribe(SOCKET_CHANNEL, this.track);
  }

  /** Runs `work` so that each socket it opens, now or later, is owned. */
  run<T>(work: () => Promise<T>): Promise<T> {
    return OWNER.run(this, work);
  }

  /** Destroys every owned socket. */
  shut(): void {
    for (const socket of this.sockets) {
      socket.destroy();
    }
  }

  /** Destroys every owned socket and stops owning new ones. */
  release(): void {
    this.shut();
    diagnosticsChannel.unsubscribe(SOCKET_CHANNEL, this.track);
  }
}
