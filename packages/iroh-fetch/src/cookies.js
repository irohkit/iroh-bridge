// A small cookie jar so cookie-based sessions work over iroh. Browsers never see these
// responses as real HTTP, so they can't store the cookies themselves. Each backend has its own
// jar and `Domain` is ignored on purpose: a cookie only ever goes back to the backend that set it.

// Browser-like limits, so a backend can't grow the jar (and every request's Cookie header)
// without bound.
const MAX_COOKIES = 180;
const MAX_COOKIE_BYTES = 4096;

export class CookieJar {
  /** @type {Map<string, { value: string, path: string, expires: number }>} */
  #cookies = new Map();
  #storage;
  #key;

  /** @param {{ storage?: Storage, key?: string }} [persist] keep cookies across reloads */
  constructor(persist) {
    this.#storage = persist?.storage;
    this.#key = persist?.key;
    if (this.#storage) {
      try {
        for (const [name, c] of JSON.parse(this.#storage.getItem(this.#key) ?? "[]")) {
          // Storage is shared with page scripts; skip anything that isn't a cookie we wrote.
          if (typeof name === "string" && typeof c?.value === "string" && typeof c.path === "string") {
            this.#cookies.set(name, { value: c.value, path: c.path, expires: Number(c.expires ?? Infinity) });
          }
        }
      } catch {}
    }
  }

  /** Stores the `Set-Cookie` headers from `[name, value]` header pairs. */
  store(headerPairs, requestPath) {
    let changed = false;
    for (const [name, value] of headerPairs) {
      if (name.toLowerCase() !== "set-cookie" || value.length > MAX_COOKIE_BYTES) continue;
      const [pair, ...attrs] = value.split(";");
      const eq = pair.indexOf("=");
      if (eq <= 0) continue;
      const key = pair.slice(0, eq).trim();
      const cookie = { value: pair.slice(eq + 1).trim(), path: defaultPath(requestPath), expires: Infinity };
      for (const attr of attrs) {
        const [k, ...rest] = attr.split("=");
        const v = rest.join("=").trim();
        switch (k.trim().toLowerCase()) {
          case "path":
            if (v.startsWith("/")) cookie.path = v;
            break;
          case "max-age":
            // RFC 6265 5.2.2: digits with an optional minus; anything else is ignored.
            if (/^-?\d+$/.test(v)) cookie.expires = Date.now() + Number(v) * 1000;
            break;
          case "expires":
            if (cookie.expires === Infinity) cookie.expires = Date.parse(v) || Infinity;
            break;
        }
      }
      if (cookie.expires <= Date.now()) this.#cookies.delete(key);
      else {
        // Replace in place, or evict the oldest one when the jar is full.
        if (!this.#cookies.has(key) && this.#cookies.size >= MAX_COOKIES) this.#cookies.delete(this.#cookies.keys().next().value);
        this.#cookies.set(key, cookie);
      }
      changed = true;
    }
    if (changed) this.#save();
  }

  /** The `Cookie` header value for a request to `path`, or `""`. */
  header(path) {
    const now = Date.now();
    return [...this.#cookies]
      .filter(([, c]) => c.expires > now && pathMatches(path, c.path))
      .sort(([, a], [, b]) => b.path.length - a.path.length)
      .map(([name, c]) => `${name}=${c.value}`)
      .join("; ");
  }

  get(name) {
    const c = this.#cookies.get(name);
    return c && c.expires > Date.now() ? c.value : undefined;
  }

  clear() {
    this.#cookies.clear();
    this.#save();
  }

  #save() {
    if (!this.#storage) return;
    try {
      const live = [...this.#cookies].filter(([, c]) => c.expires > Date.now());
      this.#storage.setItem(this.#key, JSON.stringify(live));
    } catch {}
  }
}

function defaultPath(path) {
  const i = path.lastIndexOf("/");
  return i <= 0 ? "/" : path.slice(0, i);
}

function pathMatches(path, cookiePath) {
  return path === cookiePath || (path.startsWith(cookiePath) && (cookiePath.endsWith("/") || path[cookiePath.length] === "/"));
}
