/* tslint:disable */
/* eslint-disable */
/**
 * The `ReadableStreamType` enum.
 *
 * *This API requires the following crate features to be activated: `ReadableStreamType`*
 */

export type ReadableStreamType = "bytes";

export class IntoUnderlyingByteSource {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    cancel(): void;
    pull(controller: ReadableByteStreamController): Promise<any>;
    start(controller: ReadableByteStreamController): void;
    readonly autoAllocateChunkSize: number;
    readonly type: ReadableStreamType;
}

export class IntoUnderlyingSink {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    abort(reason: any): Promise<any>;
    close(): Promise<any>;
    write(chunk: any): Promise<any>;
}

export class IntoUnderlyingSource {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    cancel(): void;
    pull(controller: ReadableStreamDefaultController): Promise<any>;
}

export class IrohClient {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Close the connection and the local endpoint.
     */
    close(): Promise<any>;
    /**
     * Open the connection now instead of on the first request.
     */
    connect(): Promise<any>;
    /**
     * Create a client that talks to the gateway with the given endpoint id.
     * `relay_url` must match the gateway's relay if it uses a custom one.
     */
    static create(remote: string, relay_url?: string | null): Promise<IrohClient>;
    /**
     * This client's own endpoint id.
     */
    endpointId(): string;
    /**
     * Send one HTTP request. `headers` is an array of `[name, value]` pairs and `body` is
     * `undefined`, a `Uint8Array`, or a `ReadableStream<Uint8Array>` that is uploaded as it is read.
     * Resolves to `{ status, headers, body: ReadableStream<Uint8Array> }`; the response body
     * streams in as the backend produces it.
     */
    request(method: string, path: string, headers: any, body: any): Promise<any>;
    /**
     * Open a WebSocket to `path` on the backend. Resolves to `{ status, headers, socket }` where
     * `socket` is set only when the backend accepted the upgrade (status 101); otherwise `body`
     * holds the backend's error response.
     */
    websocket(path: string, headers: any): Promise<any>;
}

/**
 * One WebSocket tunnelled over an iroh stream. Driven by the JS `IrohWebSocket` wrapper.
 */
export class WebSocketConn {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Sends a close frame and finishes our side of the stream.
     */
    close(code: number, reason: string): Promise<any>;
    /**
     * Resolves to the next message: `{ type: "text", data: string }`,
     * `{ type: "binary", data: Uint8Array }`, `{ type: "close", code, reason }`, or `null` at the end.
     */
    recv(): Promise<any>;
    sendBinary(data: Uint8Array): Promise<any>;
    sendText(text: string): Promise<any>;
}

/**
 * Turn on iroh's tracing output in the JS console. `level` is e.g. "info" or "debug".
 */
export function enableLogging(level: string): void;

export function start(): void;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_intounderlyingbytesource_free: (a: number, b: number) => void;
    readonly __wbg_intounderlyingsink_free: (a: number, b: number) => void;
    readonly __wbg_intounderlyingsource_free: (a: number, b: number) => void;
    readonly __wbg_irohclient_free: (a: number, b: number) => void;
    readonly __wbg_websocketconn_free: (a: number, b: number) => void;
    readonly enableLogging: (a: number, b: number) => void;
    readonly intounderlyingbytesource_autoAllocateChunkSize: (a: number) => number;
    readonly intounderlyingbytesource_cancel: (a: number) => void;
    readonly intounderlyingbytesource_pull: (a: number, b: any) => any;
    readonly intounderlyingbytesource_start: (a: number, b: any) => void;
    readonly intounderlyingbytesource_type: (a: number) => number;
    readonly intounderlyingsink_abort: (a: number, b: any) => any;
    readonly intounderlyingsink_close: (a: number) => any;
    readonly intounderlyingsink_write: (a: number, b: any) => any;
    readonly ring_core_0_17_14__bn_mul_mont: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
    readonly intounderlyingsource_cancel: (a: number) => void;
    readonly intounderlyingsource_pull: (a: number, b: any) => any;
    readonly irohclient_close: (a: number) => any;
    readonly irohclient_connect: (a: number) => any;
    readonly irohclient_create: (a: number, b: number, c: number, d: number) => any;
    readonly irohclient_endpointId: (a: number) => [number, number];
    readonly irohclient_request: (a: number, b: number, c: number, d: number, e: number, f: any, g: any) => any;
    readonly irohclient_websocket: (a: number, b: number, c: number, d: any) => any;
    readonly start: () => void;
    readonly websocketconn_close: (a: number, b: number, c: number, d: number) => any;
    readonly websocketconn_recv: (a: number) => any;
    readonly websocketconn_sendBinary: (a: number, b: any) => any;
    readonly websocketconn_sendText: (a: number, b: number, c: number) => any;
    readonly wasm_bindgen_e1363021040f656a___convert__closures_____invoke___js_sys_7533570554f6805f___Function_fn_wasm_bindgen_e1363021040f656a___JsValue_____wasm_bindgen_e1363021040f656a___sys__Undefined___js_sys_7533570554f6805f___Function_fn_wasm_bindgen_e1363021040f656a___JsValue_____wasm_bindgen_e1363021040f656a___sys__Undefined_______true_: (a: number, b: number, c: any, d: any) => void;
    readonly wasm_bindgen_e1363021040f656a___convert__closures_____invoke___wasm_bindgen_e1363021040f656a___JsValue__core_37ee0ce0c4cd57d5___result__Result_____wasm_bindgen_e1363021040f656a___JsError___true_: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen_e1363021040f656a___convert__closures_____invoke___wasm_bindgen_e1363021040f656a___JsValue______true_: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen_e1363021040f656a___convert__closures_____invoke___wasm_bindgen_e1363021040f656a___JsValue______true__25: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen_e1363021040f656a___convert__closures_____invoke___wasm_bindgen_e1363021040f656a___JsValue______true__26: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen_e1363021040f656a___convert__closures_____invoke_______true_: (a: number, b: number) => void;
    readonly wasm_bindgen_e1363021040f656a___convert__closures_____invoke_______true__1_: (a: number, b: number) => void;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __externref_drop_slice: (a: number, b: number) => void;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __wbindgen_destroy_closure: (a: number, b: number) => void;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
