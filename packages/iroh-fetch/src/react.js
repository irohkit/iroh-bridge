// React bindings: <IrohProvider> opens one iroh connection for the app, useIroh() reads it.
import { createContext, createElement, useContext, useEffect, useMemo, useState } from "react";

const IrohContext = createContext(null);

/**
 * @param {import("./react.d.ts").IrohProviderProps} props
 */
export function IrohProvider({ children, install, ...options }) {
  const [state, setState] = useState({ status: "connecting", client: null, error: null });
  const deps = [options.endpointId, JSON.stringify(options.backends ?? null), options.relayUrl, options.connectTimeout, JSON.stringify(install ?? null)];

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
      .then((c) => {
        client = c;
        if (cancelled) return c.close();
        if (install) uninstall = c.install(install === true ? undefined : install);
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
