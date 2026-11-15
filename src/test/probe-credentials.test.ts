import dns from "node:dns";

import type { ProbeAnswer } from "@antelopejs/interface-database-catalog";
import { expect } from "chai";
import sinon from "sinon";

import { probe } from "../probe";
import { type Listener, SecretOf, StartListener } from "./probe-servers";

const TIMEOUT_MS = 1000;
const MACHINE_VARIABLES = [
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "AWS_SESSION_TOKEN",
];
const ACCOUNT = `writer:${SecretOf("writer")}@`;
const KEY_FILE = "/etc/ssl/private/dms-host.pem";
const SRV_HOST = "cluster.probe.test";

/** URL options that would sign in or connect with what the probing machine holds, and what the refusal names. */
const MACHINE_OPTIONS: [string, string, string][] = [
  ["", "authMechanism=MONGODB-AWS", "MONGODB-AWS"],
  [ACCOUNT, "authmechanism=mongodb-aws", "MONGODB-AWS"],
  ["dms@", "authMechanism=GSSAPI", "GSSAPI"],
  [
    "",
    "authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s",
    "MONGODB-OIDC",
  ],
  ["", "authMechanism=MONGODB-X509&tls=true", "MONGODB-X509"],
  [
    ACCOUNT,
    `tls=true&tlsCertificateKeyFile=${KEY_FILE}`,
    "tlsCertificateKeyFile",
  ],
  [ACCOUNT, `tls=true&TLSCAFILE=${KEY_FILE}`, "tlsCAFile"],
  [ACCOUNT, `tls=true&tlsCRLFile=${KEY_FILE}`, "tlsCRLFile"],
];

/** TXT records an attacker's DNS could serve to turn the same options on. */
const MACHINE_TXT_RECORDS = [
  "authMechanism=MONGODB-AWS",
  "authMechanismProperties=ENVIRONMENT:k8s",
  `tlsCertificateKeyFile=${KEY_FILE}`,
];

function Probe(url: string): Promise<ProbeAnswer> {
  return probe({
    url,
    timeoutMs: TIMEOUT_MS,
    signal: new AbortController().signal,
  });
}

describe("probing with nothing but what the URL carries", () => {
  let host: Listener;
  const saved = new Map<string, string | undefined>();

  before(async () => {
    host = await StartListener();
    for (const name of MACHINE_VARIABLES) {
      saved.set(name, process.env[name]);
      process.env[name] = SecretOf(name);
    }
  });

  afterEach(() => {
    sinon.restore();
  });

  after(async () => {
    for (const [name, value] of saved) {
      if (value === undefined) {
        delete process.env[name];
      } else {
        process.env[name] = value;
      }
    }
    await host.close();
  });

  for (const [account, options, named] of MACHINE_OPTIONS) {
    it(`refuses ${options} before connecting`, async () => {
      const acceptedBefore = host.accepted();
      const answer = await Probe(
        `mongodb://${account}127.0.0.1:${host.port}/shop?${options}`,
      );
      expect(answer).to.include({ outcome: "refused", refusal: "other" });
      expect(answer).to.have.property("message").that.includes(named);
      expect(host.accepted()).to.equal(acceptedBefore);
    });
  }

  for (const record of MACHINE_TXT_RECORDS) {
    it(`takes no ${record.split("=")[0]} from a mongodb+srv TXT record, and connects to nothing`, async () => {
      sinon
        .stub(dns.promises, "resolveSrv")
        .resolves([
          { name: `node.${SRV_HOST}`, port: host.port, priority: 0, weight: 0 },
        ]);
      sinon.stub(dns.promises, "resolveTxt").resolves([[record]]);
      const acceptedBefore = host.accepted();
      const answer = await Probe(`mongodb+srv://${ACCOUNT}${SRV_HOST}/shop`);
      expect(answer).to.include({ outcome: "unreachable" });
      expect(answer)
        .to.have.property("message")
        .that.includes("Text record may only set");
      expect(host.accepted()).to.equal(acceptedBefore);
    });
  }
});
