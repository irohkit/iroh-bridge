// React bindings: <IrohProvider> opens one iroh connection for the app, useIroh() reads it.
import { createContext, createElement, useContext, useEffect, useMemo, useState, useSyncExternalStore } from "react";

const IrohContext = createContext(null);

/**
 * @param {import("./react.d.ts").IrohProviderProps} props
 */
export function IrohProvider({ children, install, serviceWorker, ...options }) {
  const [state, setState] = useState({ status: "connecting", client: null, error: null });
  const deps = [options.endpointId, JSON.stringify(options.backends ?? null), options.relayUrl, options.relayToken, options.connectTimeout, JSON.stringify(install ?? null), JSON.stringify(serviceWorker ?? null)];

  // Requests made before the connection is ready wait for it instead of failing.
  const ready = useMemo(() => {
    let resolve, reject;
    const promise = new Promise((res, rej) => ((resolve = res), (reject = rej)));
    promise.catch(() => {});
    return { promise, resolve, reject };
  }, deps);

  useEffect(() => {
    if (!options.endpointId && !options.backends) {
      const error = new Error("IrohProvider: endpointId (or backends) is not set");
      setState({ status: "error", client: null, error });
      ready.reject(error);
      return;
    }
    let client;
    let uninstall;
    let cancelled = false;
    setState({ status: "connecting", client: null, error: null });
    // Imported lazily so the WASM only ever loads in the browser, never during SSR.
    import("./browser.js")
      .then(({ createIrohFetch }) => createIrohFetch(options))
      .then(async (c) => {
        client = c;
        if (cancelled) return c.close();
        if (install) uninstall = c.install(install === true ? undefined : install);
        // Report ready only once the worker controls the page, so the first <img> already loads.
        // Where service workers aren't available (iOS in-app browsers such as LINE or Facebook,
        // some private modes), carry on without them: everything but browser-initiated loads
        // (<img>, <video>, CSS) still works.
        if (serviceWorker) {
          try {
            await c.serviceWorker(serviceWorker === true ? undefined : serviceWorker);
          } catch (err) {
            console.warn("iroh-fetch: service worker unavailable, <img>/<video> of backend URLs won't load:", err?.message ?? err);
          }
        }
        if (cancelled) return;
        setState({ status: "ready", client: c, error: null });
        ready.resolve(c);
      })
      .catch((error) => {
        if (cancelled) return;
        setState({ status: "error", client: null, error });
        ready.reject(error);
      });
    return () => {
      cancelled = true;
      uninstall?.();
      client?.close();
    };
  }, deps);

  const value = useMemo(() => ({ ...state, ready: ready.promise }), [state, ready]);
  return createElement(IrohContext.Provider, { value }, children);
}

/** `{ status: "connecting" | "ready" | "error", client, error }` */
export function useIroh() {
  const ctx = useContext(IrohContext);
  if (!ctx) throw new Error("useIroh() must be used inside <IrohProvider>");
  return ctx;
}

/** A fetch function that waits for the connection, so it can be called right away. */
export function useIrohFetch() {
  const { ready } = useIroh();
  return useMemo(() => (input, init) => ready.then((c) => c.fetch(input, init)), [ready]);
}

const IDLE = { url: undefined, loading: false, error: null };

/**
 * Loads `url` over iroh and returns `{ url, loading, error }`, where `url` is a `blob:` URL for
 * <img>/<video>. Works without a service worker; the blob URL is revoked on unmount or URL change.
 */
export function useIrohObjectUrl(url) {
  const { ready } = useIroh();
  const key = url == null ? null : String(url);
  const [state, setState] = useState(() => (key ? { ...IDLE, loading: true } : IDLE));
  useEffect(() => {
    if (!key) return setState(IDLE);
    let cancelled = false;
    let blobUrl;
    const controller = new AbortController();
    setState({ ...IDLE, loading: true });
    ready
      .then((c) => c.objectUrl(key, { signal: controller.signal }))
      .then((u) => {
        if (cancelled) return URL.revokeObjectURL(u);
        blobUrl = u;
        setState({ url: u, loading: false, error: null });
      })
      .catch((error) => {
        if (!cancelled) setState({ url: undefined, loading: false, error });
      });
    return () => {
      cancelled = true;
      controller.abort();
      if (blobUrl) URL.revokeObjectURL(blobUrl);
    };
  }, [key, ready]);
  return state;
}

// Whether a service worker controls the page: null during SSR and hydration (unknown), then a
// boolean that follows `controllerchange`, so images switch to the worker once it takes over.
const subscribeController = (onChange) => {
  const sw = globalThis.navigator?.serviceWorker;
  sw?.addEventListener("controllerchange", onChange);
  return () => sw?.removeEventListener("controllerchange", onChange);
};
const getControlled = () => Boolean(globalThis.navigator?.serviceWorker?.controller);
const getServerControlled = () => null;

const isAbsoluteHttp = (src) => /^https?:\/\//i.test(src);

/**
 * An <img> for a backend URL. Uses the service worker when one controls the page, and otherwise
 * (iOS in-app browsers such as LINE or Facebook, some private modes) loads the image over iroh into
 * a `blob:` URL. Relative, data: and blob: sources, and URLs of origins that aren't backends, are
 * passed through unchanged.
 */
export function IrohImage({ src, ...props }) {
  const { client } = useIroh();
  const controlled = useSyncExternalStore(subscribeController, getControlled, getServerControlled);
  const key = src == null ? null : String(src);
  let mode;
  if (key == null || !isAbsoluteHttp(key) || controlled) mode = "direct";
  // Unknown until hydrated and connected (which origins are backends comes from the client).
  else if (controlled === null || !client) mode = "wait";
  else mode = isBackendUrl(client, key) ? "iroh" : "direct";
  const { url } = useIrohObjectUrl(mode === "iroh" ? key : null);
  return createElement("img", { ...props, src: mode === "direct" ? (key ?? undefined) : mode === "iroh" ? url : undefined });
}

function isBackendUrl(client, src) {
  const origins = client.backends.map((b) => b.origin).filter(Boolean);
  // A single endpointId has no origin: every URL goes to it, as with iroh.fetch.
  if (!origins.length) return true;
  try {
    return origins.includes(new URL(src).origin);
  } catch {
    return false;
  }
}
