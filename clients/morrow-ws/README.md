# Morrow WebSocket client

This browser client uses the separate `morrow.v1.text` WebSocket listener and
the existing Morrow text session protocol. It supports connection state events,
publish, subscriptions (including queue groups), request/reply, explicit ACKs,
reconnection, and a bounded receive queue.

```ts
const client = new MorrowWebSocketClient({
  url: "wss://broker.example/ws",
  durableId: "browser-client",
});
client.addEventListener("state", (event) => console.log(event));
await client.connect();
client.subscribe("orders/*", "orders");
client.publish("orders/created", JSON.stringify({ id: 42 }));
```

Authentication accepts an application-provided Ed25519 nonce signer through
`auth.signNonce`; this keeps private keys out of the client library and works
with WebCrypto or a platform wallet implementation.
