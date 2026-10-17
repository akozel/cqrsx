// Minimal fetch wrapper: same origin, JSON in and out, errors as values where the API uses them.
export class HttpError extends Error {
  constructor(
    public status: number,
    public body: unknown,
  ) {
    super(`HTTP ${status}`)
  }
}

export async function getJson<T>(url: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(url, { headers: { accept: 'application/json' }, signal })
  const body = await response.json().catch(() => null)
  if (!response.ok) throw new HttpError(response.status, body)
  return body as T
}

export async function postJson<T>(url: string, payload?: unknown): Promise<{ status: number; body: T }> {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json', accept: 'application/json' },
    body: payload === undefined ? undefined : JSON.stringify(payload),
  })
  const body = (await response.json().catch(() => null)) as T
  return { status: response.status, body }
}
