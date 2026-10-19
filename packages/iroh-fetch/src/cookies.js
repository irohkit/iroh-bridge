// A small cookie jar so cookie-based sessions work over iroh. Browsers never see these
// responses as real HTTP, so they can't store the cookies themselves.

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
        for (const [name, c] of JSON.parse(this.#storage.getItem(this.#key) ?? "[]")) this.#cookies.set(name, c);
      } catch {}
    }
  }

  /** Stores the `Set-Cookie` headers from `[name, value]` header pairs. */
  store(headerPairs, requestPath) {
    let changed = false;
    for (const [name, value] of headerPairs) {
      if (name.toLowerCase() !== "set-cookie") continue;
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
            cookie.expires = Date.now() + Number(v) * 1000;
            break;
          case "expires":
            if (cookie.expires === Infinity) cookie.expires = Date.parse(v) || Infinity;
            break;
        }
      }
      if (cookie.expires <= Date.now()) this.#cookies.delete(key);
      else this.#cookies.set(key, cookie);
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
