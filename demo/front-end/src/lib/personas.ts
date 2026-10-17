// The cast and the crowd. Crowd avatars are derived deterministically from the fan id.
import type { Persona } from '@/types/domain'

export const DEFAULT_PERSONAS: Persona[] = [
  { fan_id: 'fox', name: 'Fox', avatar: '🦊' },
  { fan_id: 'panda', name: 'Panda', avatar: '🐼' },
  { fan_id: 'unicorn', name: 'Unicorn', avatar: '🦄' },
  { fan_id: 'arbitrage-bot', name: 'Arbitrage Bot', avatar: '🤖' },
]

/** Sixty distinct creatures for `crowd-1…crowd-60` (the crowd action draws from that range), none of
 * them a cast avatar, so every guest in the picker looks different. */
const CROWD_AVATARS = [
  '🐸',
  '🐙',
  '🦉',
  '🐧',
  '🦁',
  '🐨',
  '🦋',
  '🐢',
  '🦜',
  '🐝',
  '🦔',
  '🐬',
  '🦩',
  '🐳',
  '🦚',
  '🐞',
  '🐯',
  '🐮',
  '🐷',
  '🐵',
  '🐔',
  '🐥',
  '🦆',
  '🦅',
  '🦇',
  '🐺',
  '🐗',
  '🐴',
  '🦓',
  '🦒',
  '🐘',
  '🦏',
  '🦛',
  '🐭',
  '🐹',
  '🐰',
  '🐻',
  '🐶',
  '🐱',
  '🐲',
  '🦎',
  '🐍',
  '🐊',
  '🦖',
  '🦕',
  '🐌',
  '🦗',
  '🕷️',
  '🦂',
  '🐠',
  '🐡',
  '🦈',
  '🐋',
  '🦭',
  '🦦',
  '🦥',
  '🦘',
  '🦙',
  '🐿️',
  '🦫',
]
const CROWD_NAMES = [
  'Mia',
  'Noah',
  'Zoe',
  'Liam',
  'Ava',
  'Leo',
  'Ivy',
  'Max',
  'Uma',
  'Eli',
  'Nia',
  'Kai',
  'Lia',
  'Ori',
  'Pia',
  'Rex',
]

export function hashString(input: string): number {
  let hash = 2166136261
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i)
    hash = Math.imul(hash, 16777619) >>> 0
  }
  return hash >>> 0
}

/** `crowd-<n>` → n, or 0 for any other id. */
function crowdNumber(fanId: string): number {
  const m = /^crowd-(\d+)$/.exec(fanId)
  return m ? Number(m[1]) : 0
}

/** Crowd fans get an avatar by their number (unique across `crowd-1…60`); other ids fall back to a hash. */
export function crowdAvatar(fanId: string): string {
  const n = crowdNumber(fanId)
  const index = n > 0 ? (n - 1) % CROWD_AVATARS.length : hashString(fanId) % CROWD_AVATARS.length
  return CROWD_AVATARS[index] ?? '🙂'
}

export function crowdName(fanId: string): string {
  const n = crowdNumber(fanId)
  return `${CROWD_NAMES[hashString(fanId) % CROWD_NAMES.length] ?? 'Fan'} #${n}`
}

export function avatarOf(fanId: string, personas: Persona[] = DEFAULT_PERSONAS): string {
  return personas.find((p) => p.fan_id === fanId)?.avatar ?? crowdAvatar(fanId)
}

export function nameOf(fanId: string, personas: Persona[] = DEFAULT_PERSONAS): string {
  return (
    personas.find((p) => p.fan_id === fanId)?.name ?? (fanId.startsWith('crowd-') ? crowdName(fanId) : fanId)
  )
}
