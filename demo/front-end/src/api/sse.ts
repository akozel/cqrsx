// A tiny EventSource wrapper with automatic reconnection (the browser reconnects on its own for
// network errors; this also survives the back-end restarting with a different backlog).
export interface SseHandle {
  close(): void
}

export function subscribe(
  url: string,
  handlers: Record<string, (data: string, event: MessageEvent) => void>,
  onState?: (connected: boolean) => void,
): SseHandle {
  let source: EventSource | null = null
  let closed = false
  let retryTimer: ReturnType<typeof setTimeout> | null = null
  const open = () => {
    if (closed) return
    source = new EventSource(url)
    source.onopen = () => onState?.(true)
    for (const [name, handler] of Object.entries(handlers)) {
      source.addEventListener(name, (event) => handler((event as MessageEvent).data, event as MessageEvent))
    }
    source.onerror = () => {
      onState?.(false)
      // EventSource retries by itself; if it gave up (CLOSED), start again after a pause.
      if (source?.readyState === EventSource.CLOSED && !closed) {
        retryTimer = setTimeout(open, 2000)
      }
    }
  }
  open()
  return {
    close() {
      closed = true
      if (retryTimer) clearTimeout(retryTimer)
      source?.close()
    },
  }
}
