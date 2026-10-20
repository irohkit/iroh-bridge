// Needs a running iroh-gateway: IROH_ENDPOINT_ID=<id> npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { createIrohFetch } from "iroh-fetch";

const endpointId = process.env.IROH_ENDPOINT_ID;

test("GET and POST through the gateway", { skip: !endpointId, timeout: 60_000 }, async () => {
  const iroh = await createIrohFetch({ endpointId, relayUrl: process.env.IROH_RELAY_URL, relayToken: process.env.IROH_RELAY_TOKEN });
  try {
    const get = await iroh.fetch("/hello?x=1");
    assert.equal(get.status, 200);
    assert.equal((await get.json()).url, "/hello?x=1");

    const post = await iroh.fetch("/items", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ a: 1 }),
    });
    const data = await post.json();
    assert.equal(data.method, "POST");
    assert.equal(data.body, '{"a":1}');

    // Requests share one connection and run concurrently on separate streams.
    const many = await Promise.all(Array.from({ length: 20 }, (_, i) => iroh.fetch(`/n/${i}`).then((r) => r.json())));
    assert.deepEqual(many.map((r) => r.url), Array.from({ length: 20 }, (_, i) => `/n/${i}`));
  } finally {
    await iroh.close();
  }
});
