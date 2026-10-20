import type { ImgHTMLAttributes, ReactElement, ReactNode } from "react";
import type { InstallOptions, IrohFetch, IrohFetchOptions } from "./index";

export interface IrohProviderProps extends Partial<IrohFetchOptions> {
  children?: ReactNode;
  /**
   * See `IrohFetchOptions.relayToken`; best a function that asks your backend. It may be written
   * inline: a new function on re-render doesn't reconnect, the latest one is called when needed.
   */
  relayToken?: string | (() => string | Promise<string>);
  /**
   * Route the global fetch / WebSocket / EventSource through iroh while mounted, so existing
   * code and libraries (SWR, React Query, axios with the fetch adapter...) work unchanged.
   */
  install?: boolean | InstallOptions;
  /**
   * Register the iroh service worker (serve node_modules/iroh-fetch/src/sw.js at /iroh-sw.js) so
   * <img>, <video>, CSS and downloads of backend URLs work too. `true` or `{ url, scope }`.
   */
  serviceWorker?: boolean | { url?: string; scope?: string };
}

export type IrohState =
  | { status: "connecting"; client: null; error: null; ready: Promise<IrohFetch> }
  | { status: "ready"; client: IrohFetch; error: null; ready: Promise<IrohFetch> }
  | { status: "error"; client: null; error: Error; ready: Promise<IrohFetch> };

export function IrohProvider(props: IrohProviderProps): ReactElement;
export function useIroh(): IrohState;
/** A fetch that waits for the connection, so it can be used before the client is ready. */
export function useIrohFetch(): (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

export interface IrohObjectUrl {
  /** A `blob:` URL of the loaded body, or undefined while loading, on error, or without a URL. */
  url: string | undefined;
  loading: boolean;
  error: Error | null;
}

/**
 * Load `url` over iroh and get a `blob:` URL for `<img>` / `<video>`, without a service worker
 * (iOS in-app browsers such as LINE or Facebook, some private modes). The blob URL is revoked when
 * the component unmounts or `url` changes. Pass null/undefined to load nothing.
 */
export function useIrohObjectUrl(url: string | URL | null | undefined): IrohObjectUrl;

/**
 * `<img>` for a backend URL that works with or without the service worker: the browser loads it
 * through the worker when one controls the page, otherwise the image is fetched over iroh into a
 * `blob:` URL. Relative, data: and blob: sources and non-backend origins are passed through.
 */
export function IrohImage(props: ImgHTMLAttributes<HTMLImageElement>): ReactElement;
