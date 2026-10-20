// relayToken as a function: fetched at runtime, fetched again when the relay refuses it, and on a
// timer. Needs a token-protected relay and a gateway on it:
// IROH_RELAY_URL=... IROH_RELAY_TOKEN=... IROH_ENDPOINT_ID=<id> npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { createIrohFetch } from "iroh-fetch";

const endpointId = process.env.IROH_ENDPOINT_ID;
const relayUrl = process.env.IROH_RELAY_URL;
const goodToken = process.env.IROH_RELAY_TOKEN;
const opts = { skip: !(endpointId && relayUrl && goodToken), timeout: 90_000 };

/** A relayToken function that hands out `tokens` in turn (the last one from then on). */
function tokenSequence(...tokens) {
  const fn = async () => {
    fn.calls++;
    return tokens[Math.min(fn.calls, tokens.length) - 1];
  };
  fn.calls = 0;
  return fn;
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test("relayToken as a function is called before connecting and works", opts, async () => {
  const relayToken = tokenSequence(goodToken);
  const iroh = await createIrohFetch({ endpointId, relayUrl, relayToken });
  try {
    assert.equal(relayToken.calls, 1);
    const who = await (await iroh.fetch("/whoami")).json();
    assert.equal(who.remoteId, iroh.endpointId);
    assert.equal(relayToken.calls, 1, "no refresh while the token is accepted");
  } finally {
    await iroh.close();
  }
});

test("a refused token is replaced by calling relayToken again", opts, async () => {
  const relayToken = tokenSequence("not-the-relay-token", goodToken);
  const iroh = await createIrohFetch({ endpointId, relayUrl, relayToken });
  try {
    assert.equal(relayToken.calls, 2, "the refusal fetched a new token");
    const who = await (await iroh.fetch("/whoami")).json();
    assert.equal(who.remoteId, iroh.endpointId, "the endpoint id survives the new client");
  } finally {
    await iroh.close();
  }
});

test("refreshRelayToken() replaces the client; a later refusal recovers on the next request", opts, async () => {
  const relayToken = tokenSequence(goodToken, "rotated-away", goodToken);
  const iroh = await createIrohFetch({ endpointId, relayUrl, relayToken });
  try {
    const id = iroh.endpointId;
    await (await iroh.fetch("/login")).text();
    assert.equal(iroh.cookies.get("session"), "abc123");
    assert.equal(await iroh.refreshRelayToken(), true, "a different token replaces the client");
    // The new client has the bad token: the first dial is refused, which fetches the good one again.
    const who = await (await iroh.fetch("/whoami")).json();
    assert.equal(relayToken.calls, 3);
    assert.equal(iroh.endpointId, id);
    assert.equal(who.remoteId, id, "same endpoint id after recreating the client");
    assert.match(who.cookie ?? "", /session=abc123/, "cookies outlive the client");
    assert.equal(await iroh.refreshRelayToken(), false, "same token: nothing to do");
  } finally {
    await iroh.close();
  }
});

test("relayTokenRefreshMs calls relayToken on a timer, and close() stops it", { skip: !relayUrl }, async () => {
  const relayToken = tokenSequence("t");
  const iroh = await createIrohFetch({
    endpointId: "0".repeat(64),
    relayUrl,
    relayToken,
    relayTokenRefreshMs: 50,
    connect: false,
  });
  await sleep(400);
  const before = relayToken.calls;
  assert.ok(before >= 3, `timer called relayToken ${before} times`);
  await iroh.close();
  const atClose = relayToken.calls;
  await sleep(300);
  assert.equal(relayToken.calls, atClose, "no calls after close()");
});

test("a relayToken that throws fails createIrohFetch", { skip: !relayUrl }, async () => {
  await assert.rejects(
    createIrohFetch({ endpointId: "0".repeat(64), relayUrl, relayToken: () => Promise.reject(new Error("not logged in")), connect: false }),
    /not logged in/,
  );
});
