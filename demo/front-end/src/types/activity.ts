// One line of the back-end activity log (SSE /api/activity).
export interface ActivityEntry {
  id: number
  at: string
  type: string
  [key: string]: unknown
}

export interface ActivityHello {
  config: unknown
  backlog: ActivityEntry[]
}
