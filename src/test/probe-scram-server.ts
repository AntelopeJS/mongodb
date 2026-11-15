import net from "node:net";

import { Binary, BSON, type Document } from "mongodb";

import { isDocument, isString } from "../implementations/database/utils";

const OP_QUERY = 2004;
const OP_MSG = 2013;
const HEADER_BYTES = 16;
const OP_MSG_BODY_OFFSET = 21;
const OP_QUERY_SKIPPED_BYTES = 8;
const AUTHENTICATION_FAILED = 18;
const SALT = Buffer.from("probe-scram-salt").toString("base64");

/** A fake MongoDB server and how to close it. */
export interface ScramServer {
  port: number;
  close: () => Promise<void>;
}

function Reply(requestId: number, document: Document): Buffer {
  const body = BSON.serialize(document);
  const header = Buffer.alloc(OP_MSG_BODY_OFFSET);
  header.writeInt32LE(OP_MSG_BODY_OFFSET + body.length, 0);
  header.writeInt32LE(requestId, 8);
  header.writeInt32LE(OP_MSG, 12);
  return Buffer.concat([header, body]);
}

/** The command document of an OP_MSG or OP_QUERY message. */
function CommandOf(message: Buffer): Document {
  if (message.readInt32LE(12) !== OP_QUERY) {
    return BSON.deserialize(message.subarray(OP_MSG_BODY_OFFSET));
  }
  const name = message.indexOf(0, HEADER_BYTES + 4);
  return BSON.deserialize(message.subarray(name + 1 + OP_QUERY_SKIPPED_BYTES));
}

/** The server's first SCRAM message to `request`: the client's nonce, a salt, and `iterations`. */
function ScramStart(request: Document, iterations: number): Document {
  const payload: unknown = request.payload;
  const text =
    payload instanceof Binary ? Buffer.from(payload.buffer).toString() : "";
  const nonce = /r=([^,]+)/.exec(text)?.[1] ?? "";
  return {
    conversationId: 1,
    done: false,
    payload: new Binary(
      Buffer.from(`r=${nonce}server,s=${SALT},i=${iterations}`),
    ),
  };
}

function Answer(command: Document, iterations: number): Document {
  const speculative: unknown = command.speculativeAuthenticate;
  if (command.saslStart !== undefined) {
    return { ...ScramStart(command, iterations), ok: 1 };
  }
  if (["hello", "ismaster", "isMaster"].some((name) => name in command)) {
    const hello: Document = {
      ismaster: true,
      isWritablePrimary: true,
      helloOk: true,
      maxWireVersion: 21,
      minWireVersion: 0,
      ok: 1,
    };
    if (isDocument(speculative)) {
      hello.speculativeAuthenticate = ScramStart(speculative, iterations);
    }
    return hello;
  }
  return { ok: 0, errmsg: "fake server", code: AUTHENTICATION_FAILED };
}

/**
 * A fake MongoDB server whose first SCRAM message asks the client to derive the password with
 * `iterations` iterations, as a hostile server can. It refuses every later SCRAM step.
 */
export async function StartScramServer(
  iterations: number,
): Promise<ScramServer> {
  const sockets = new Set<net.Socket>();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => undefined);
    let pending = Buffer.alloc(0);
    socket.on("data", (chunk) => {
      pending = Buffer.concat([pending, Buffer.from(chunk)]);
      while (pending.length >= 4 && pending.length >= pending.readInt32LE(0)) {
        const message = pending.subarray(0, pending.readInt32LE(0));
        pending = pending.subarray(message.length);
        socket.write(
          Reply(message.readInt32LE(4), Answer(CommandOf(message), iterations)),
        );
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (address === null || isString(address)) {
    throw new Error("The fake server listens on no TCP port");
  }
  return {
    port: address.port,
    close: async () => {
      for (const socket of sockets) {
        socket.destroy();
      }
      await new Promise((resolve) => server.close(resolve));
    },
  };
}
