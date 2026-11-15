import { randomBytes } from "node:crypto";
import diagnosticsChannel from "node:diagnostics_channel";
import net from "node:net";

import { isDocument, isString } from "../implementations/database/utils";

const SETTLE_POLL_MS = 20;

/** How long a test lets the sockets settle before it counts those still open. */
export const CLOSE_SETTLE_MS = 1000;

const RUN_SECRET = randomBytes(6).toString("hex");

/** A throwaway password for `name`, random for each test run. */
export function SecretOf(name: string): string {
  return `${name}-${RUN_SECRET}`;
}

function PortOf(server: net.Server): number {
  const address = server.address();
  if (address === null || isString(address)) {
    throw new Error("The server listens on no TCP port");
  }
  return address.port;
}

async function Listening(server: net.Server): Promise<number> {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return PortOf(server);
}

async function ListeningAt(server: net.Server, socketPath: string) {
  await new Promise<void>((resolve) => server.listen(socketPath, resolve));
}

/** A server, and how to close it with every socket it accepted. */
interface Served {
  server: net.Server;
  close: () => Promise<void>;
}

function Serving(
  options: net.ServerOpts,
  accept: (socket: net.Socket, track: (socket: net.Socket) => void) => void,
): Served {
  const sockets = new Set<net.Socket>();
  const track = (socket: net.Socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => undefined);
  };
  const server = net.createServer(options, (socket) => accept(socket, track));
  return {
    server,
    close: async () => {
      for (const socket of sockets) {
        socket.destroy();
      }
      await new Promise((resolve) => server.close(resolve));
    },
  };
}

/** A local port nothing listens on. */
export async function ClosedPort(): Promise<number> {
  const server = net.createServer();
  const port = await Listening(server);
  await new Promise((resolve) => server.close(resolve));
  return port;
}

/** A host that accepts connections and never answers, and counts them. */
export interface Listener {
  port: number;
  accepted: () => number;
  close: () => Promise<void>;
}

/** Starts a {@link Listener} on a free local port. */
export async function StartListener(): Promise<Listener> {
  let accepted = 0;
  const { server, close } = Serving({}, (socket, track) => {
    accepted += 1;
    track(socket);
  });
  return { port: await Listening(server), accepted: () => accepted, close };
}

/** A socket file that accepts connections and never answers; returns how to close it. */
export async function StartSilentSocket(
  socketPath: string,
): Promise<() => Promise<void>> {
  const { server, close } = Serving({}, (socket, track) => track(socket));
  await ListeningAt(server, socketPath);
  return close;
}

/**
 * What a relay asks of each chunk a connection carries: `true` once the connection has gone
 * quiet, from this chunk on. A quiet connection forwards nothing more and closes nothing.
 */
export type Stall = (chunk: Buffer, fromClient: boolean) => boolean;

/** A relay in front of a database server. */
export interface Relay {
  port: number;
  close: () => Promise<void>;
}

/**
 * Forwards each connection to the server on `targetPort` until `stall` answers `true`, as a server
 * does that hangs mid-conversation or ignores the client leaving.
 */
function Relaying(targetPort: number, stall: Stall): Served {
  return Serving({ allowHalfOpen: true }, (client, track) => {
    const upstream = net.connect(targetPort, "127.0.0.1");
    let stalled = false;
    const forward = (chunk: Buffer, fromClient: boolean) => {
      stalled = stalled || stall(chunk, fromClient);
      if (!stalled) {
        (fromClient ? upstream : client).write(chunk);
      }
    };
    track(client);
    track(upstream);
    client.on("data", (chunk) => forward(Buffer.from(chunk), true));
    upstream.on("data", (chunk) => forward(Buffer.from(chunk), false));
  });
}

export async function StartRelay(
  targetPort: number,
  stall: Stall = () => false,
): Promise<Relay> {
  const { server, close } = Relaying(targetPort, stall);
  return { port: await Listening(server), close };
}

/** A socket file that relays each connection to the server on `targetPort`; returns how to close it. */
export async function StartSocketRelay(
  targetPort: number,
  socketPath: string,
): Promise<() => Promise<void>> {
  const { server, close } = Relaying(targetPort, () => false);
  await ListeningAt(server, socketPath);
  return close;
}

/** The client sockets this process opens to one port, whichever library opens them. */
export interface ClientSockets {
  /** How many are not destroyed once all have been, or after `settleMs`. */
  leftOpenAfter: (settleMs: number) => Promise<number>;
  /** How many connected so far. */
  opened: () => number;
  stop: () => void;
}

/** Watches the client sockets this process opens to `port` from now on. */
export function WatchClientSockets(port: number): ClientSockets {
  const sockets = new Set<net.Socket>();
  const track: diagnosticsChannel.ChannelListener = (message) => {
    const socket: unknown = isDocument(message) ? message.socket : undefined;
    if (socket instanceof net.Socket) {
      socket.once("connect", () => {
        if (socket.remotePort === port) {
          sockets.add(socket);
        }
      });
    }
  };
  const leftOpen = () =>
    [...sockets].filter((socket) => !socket.destroyed).length;
  diagnosticsChannel.subscribe("net.client.socket", track);
  return {
    leftOpenAfter: async (settleMs) => {
      const until = Date.now() + settleMs;
      while (leftOpen() > 0 && Date.now() < until) {
        await new Promise((resolve) => setTimeout(resolve, SETTLE_POLL_MS));
      }
      return leftOpen();
    },
    opened: () => sockets.size,
    stop: () => diagnosticsChannel.unsubscribe("net.client.socket", track),
  };
}
