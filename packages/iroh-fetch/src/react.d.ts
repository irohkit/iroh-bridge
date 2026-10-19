import type { ReactNode } from "react";
import type { InstallOptions, IrohFetch, IrohFetchOptions } from "./index.d.ts";

export interface IrohProviderProps extends Partial<IrohFetchOptions> {
  children?: ReactNode;
  /**
   * Route the global fetch / WebSocket / EventSource through iroh while mounted, so existing
   * code and libraries (SWR, React Query, axios with the fetch adapter...) work unchanged.
   */
  install?: boolean | InstallOptions;
}

export type IrohState =
  | { status: "connecting"; client: null; error: null; ready: Promise<IrohFetch> }
  | { status: "ready"; client: IrohFetch; error: null; ready: Promise<IrohFetch> }
  | { status: "error"; client: null; error: Error; ready: Promise<IrohFetch> };

export function IrohProvider(props: IrohProviderProps): JSX.Element;
export function useIroh(): IrohState;
/** A fetch that waits for the connection, so it can be used before the client is ready. */
export function useIrohFetch(): (input: string | URL | Request, init?: RequestInit) => Promise<Response>;
