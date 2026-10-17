// The cast and the crowd. Crowd avatars are derived deterministically from the fan id.
import type { Persona } from '@/types/domain'

export const DEFAULT_PERSONAS: Persona[] = [
  { fan_id: 'alice', name: 'Alice', avatar: '🦊' },
  { fan_id: 'bob', name: 'Bob', avatar: '🐼' },
  { fan_id: 'chloe', name: 'Chloe', avatar: '🦄' },
  { fan_id: 'scalper-bot', name: 'Scalper Bot', avatar: '🤖' },
]

const CROWD_AVATARS = ['🐸', '🐙', '🦉', '🐧', '🦁', '🐨', '🦋', '🐢', '🦜', '🐝', '🦔', '🐬', '🦩', '🐳', '🦚', '🐞']
const CROWD_NAMES = ['Mia', 'Noah', 'Zoe', 'Liam', 'Ava', 'Leo', 'Ivy', 'Max', 'Uma', 'Eli', 'Nia', 'Kai', 'Lia', 'Ori', 'Pia', 'Rex']

export function hashString(input: string): number {
  let hash = 2166136261
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i)
    hash = Math.imul(hash, 16777619) >>> 0
  }
  return hash >>> 0
}

export function crowdAvatar(fanId: string): string {
  return CROWD_AVATARS[hashString(fanId) % CROWD_AVATARS.length] ?? '🙂'
}

export function crowdName(fanId: string): string {
  const n = Number(fanId.replace('crowd-', '')) || 0
  return `${CROWD_NAMES[hashString(fanId) % CROWD_NAMES.length] ?? 'Fan'} #${n}`
}

export function avatarOf(fanId: string, personas: Persona[] = DEFAULT_PERSONAS): string {
  return personas.find((p) => p.fan_id === fanId)?.avatar ?? crowdAvatar(fanId)
}

export function nameOf(fanId: string, personas: Persona[] = DEFAULT_PERSONAS): string {
  return personas.find((p) => p.fan_id === fanId)?.name ?? (fanId.startsWith('crowd-') ? crowdName(fanId) : fanId)
}
