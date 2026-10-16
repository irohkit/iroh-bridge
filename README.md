# iroh-bridge

ให้ frontend (Next.js บน Vercel หรือเว็บโฮสต์ไหนก็ได้) และ Node.js ส่ง HTTP request ไปหา backend ที่อยู่หลัง NAT ได้
โดยไม่ต้องเปิดพอร์ต ไม่ต้องมี IP สาธารณะ และไม่ต้องใช้ tunnel อย่าง ngrok ทั้งหมดทำงานบน [iroh](https://www.iroh.computer/)

```
Browser / Node.js                     iroh relay                     เครื่อง backend (หลัง NAT)
┌──────────────────┐   WebSocket   ┌──────────────┐   QUIC/UDP   ┌───────────────┐  HTTP  ┌─────────┐
│ iroh-fetch (WASM)│ ────────────▶ │ n0 หรือของเรา │ ◀─────────── │ iroh-gateway  │ ─────▶ │ backend │
└──────────────────┘               └──────────────┘  (ต่อออกไป)   └───────────────┘        └─────────┘
```

- **`crates/gateway`** (`iroh-gateway`): โปรแกรม Rust ที่รันข้าง backend เดิม รับ request ผ่าน iroh แล้วส่งต่อไปที่ `BACKEND_URL`
  ทุกการเชื่อมต่อเป็นแบบ "ต่อออกไป" จึงใช้ได้หลัง NAT/CGNAT/firewall ทุกแบบ
- **`packages/iroh-fetch`**: npm package (Rust→WASM) ที่ให้ `fetch()` หน้าตาเหมือนของมาตรฐาน ใช้ได้ทั้งใน browser และ Node.js ≥ 22
- **`crates/proto`**: รูปแบบข้อมูลที่ใช้ร่วมกัน: 1 request (หรือ 1 WebSocket) ต่อ 1 QUIC stream และหลายตัววิ่งพร้อมกันบน connection เดียวได้
- **`examples/nextjs`**: แอปตัวอย่าง (provider + hook `useIroh()`)
- **`examples/backend`**: backend จำลองไว้ใช้ลอง (มี JSON echo, `/stream`, `/sse`, WebSocket echo ที่ `/ws`)

> browser ส่ง UDP ไม่ได้ ฝั่ง client จึงต่อผ่าน relay เสมอ (เข้ารหัส end-to-end; relay อ่านข้อมูลไม่ได้)
> ที่ทดสอบผ่าน relay สาธารณะของ n0 ได้ latency ประมาณ 70–120 ms ต่อ request หลังเชื่อมต่อแล้ว

## เริ่มใช้งาน

### 1. ฝั่ง backend: รัน gateway

```bash
cargo build --release -p iroh-gateway
```

```bash
BACKEND_URL=http://127.0.0.1:8080 ./target/release/iroh-gateway
```

ตอนเริ่มทำงานจะพิมพ์ `IROH_ENDPOINT_ID=...` ออกมา ค่านี้คือ "ที่อยู่" ของ backend
ค่าจะไม่เปลี่ยน ตราบใดที่ยังเก็บไฟล์ `iroh-gateway.key` ไว้ (ห้ามหลุดไปไหน เพราะใครมีไฟล์นี้ก็ปลอมเป็น backend ได้)

| ตัวแปร | ค่าเริ่มต้น | ความหมาย |
|---|---|---|
| `BACKEND_URL` | `http://127.0.0.1:8080` | HTTP backend ที่จะส่งต่อ request ไปให้ |
| `IROH_KEY_FILE` | `iroh-gateway.key` | ไฟล์ secret key ของ endpoint |
| `IROH_RELAY_URL` | (relay ของ n0) | ใช้ relay ของเราเอง |
| `MAX_BODY_BYTES` | 32 MiB | ขนาด request body สูงสุด |

ตัวอย่าง systemd unit สำหรับรันถาวร:

```ini
[Service]
Environment=BACKEND_URL=http://127.0.0.1:8080
Environment=IROH_KEY_FILE=/var/lib/iroh-gateway/key
ExecStart=/usr/local/bin/iroh-gateway
Restart=always
```

### 2. ฝั่ง frontend: ใช้ `iroh-fetch`

```ts
import { createIrohFetch } from "iroh-fetch";

const iroh = await createIrohFetch({ endpointId: process.env.NEXT_PUBLIC_IROH_ENDPOINT_ID! });
const res = await iroh.fetch("/api/users?page=2");       // คืนค่าเป็น Response มาตรฐาน
const users = await res.json();
await iroh.fetch("/api/users", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ name: "a" }) });
```

**Streaming response** (SSE, NDJSON, ไฟล์ใหญ่): body ไหลมาเป็นช่วง ๆ ตามที่ backend ส่ง และยกเลิกได้ด้วย `AbortController`
(gateway จะหยุดดึงข้อมูลจาก backend ทันที)

```ts
const ctrl = new AbortController();
const res = await iroh.fetch("/api/events", { signal: ctrl.signal });
const reader = res.body!.pipeThrough(new TextDecoderStream()).getReader();
for (;;) {
  const { value, done } = await reader.read();
  if (done) break;
  console.log(value);
}
// ctrl.abort() เพื่อหยุด
```

**WebSocket**: ได้ object ที่ใช้ API เดียวกับ `WebSocket` มาตรฐาน (`onopen`, `onmessage`, `send`, `close`, `binaryType`, `protocol`, `addEventListener`)
gateway จะเปิด WebSocket จริงไปที่ `ws://<BACKEND_URL>/<path>` ให้

```ts
const ws = iroh.websocket("/ws", {
  protocols: ["chat.v1"],
  headers: { authorization: `Bearer ${token}` }, // ทำแบบนี้ได้ ซึ่ง WebSocket ปกติใน browser ทำไม่ได้
});
ws.onmessage = (e) => console.log(e.data);
ws.onopen = () => ws.send("hello");
```

ใน Next.js ให้สร้าง client ครั้งเดียวในฝั่ง browser ดู [`examples/nextjs/lib/iroh.tsx`](examples/nextjs/lib/iroh.tsx)
(import แบบ dynamic ใน `useEffect` เพื่อไม่ให้โหลด WASM ตอนทำ SSR)

```bash
cd examples/nextjs && cp .env.example .env.local   # ใส่ NEXT_PUBLIC_IROH_ENDPOINT_ID
npm install && npm run dev
```

### 3. Deploy บน Vercel

แอปไม่ต้องมี server ของตัวเอง จะ deploy แบบ static หรือขึ้น Vercel/Netlify/Cloudflare Pages ก็ได้
แค่ตั้ง `NEXT_PUBLIC_IROH_ENDPOINT_ID` ใน Environment Variables ของโปรเจกต์
ถ้ายังไม่ได้ publish `iroh-fetch` ขึ้น npm ให้ commit โฟลเดอร์ `packages/iroh-fetch/wasm/` ไว้ใน repo และตั้ง Root Directory เป็น `examples/nextjs`

**ใช้จาก Node.js (API route / server)**: ใช้ได้ (ต้อง Node ≥ 22) แต่ใน serverless ทุกครั้งที่ cold start จะต้องเชื่อมต่อใหม่ (~1 วินาที)
ถ้าจะเรียกจากฝั่ง server บ่อย ๆ ให้เก็บ client ไว้ในตัวแปรระดับ module เพื่อใช้ซ้ำ หรือรันบน server ที่ทำงานตลอด

## Build package

ต้องมี Rust, target `wasm32-unknown-unknown`, `clang` (สำหรับ crate `ring`), `wasm-bindgen-cli` เวอร์ชันเดียวกับใน `Cargo.lock` และ `wasm-opt` (มีหรือไม่มีก็ได้)

```bash
./scripts/build-wasm.sh
```

ทดสอบ end-to-end (ต้องรัน `examples/backend` และ gateway ก่อน ครอบคลุม HTTP, streaming, SSE abort และ WebSocket):

```bash
cd packages/iroh-fetch && IROH_ENDPOINT_ID=<id> npm test
```

## สำหรับ production

- **รัน relay ของตัวเอง**: relay สาธารณะของ n0 เหมาะกับการพัฒนา แต่ไม่มี SLA และอาจโดน rate limit
  ให้รัน [`iroh-relay`](https://docs.iroh.computer/) บน VPS ที่มีโดเมน และ TLS จากนั้นตั้ง `IROH_RELAY_URL` ฝั่ง gateway และ `relayUrl` ฝั่ง client ให้เป็นค่าเดียวกัน
  หรือใช้บริการ relay แบบเสียเงินของ n0
- **ความปลอดภัย**: ใครรู้ endpoint id ก็ส่ง request ถึง backend ได้ (เหมือน URL สาธารณะ) ดังนั้น backend ต้องตรวจ auth เอง (เช่น `Authorization` header ซึ่ง gateway ส่งต่อไปให้ครบ)
  ข้อมูลระหว่างทางเข้ารหัสด้วย QUIC/TLS แบบ end-to-end
- **ขนาด WASM**: ~3.4 MB (gzip ~1.4 MB) โหลดครั้งเดียวแล้ว browser cache ไว้ จึงควรโหลดแบบ lazy อย่างในตัวอย่าง
- **ข้อจำกัดตอนนี้**: request body (ฝั่งส่งขึ้น) ยังถูกอ่านทั้งก้อนก่อนส่ง (สูงสุด `MAX_BODY_BYTES`) ส่วน response body stream ได้แล้ว
