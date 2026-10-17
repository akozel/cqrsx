import { createI18n } from 'vue-i18n'
import en from './en'
import ru from './ru'

export type MessageSchema = typeof en

export function detectLocale(): 'en' | 'ru' {
  try {
    const saved = localStorage.getItem('encore.locale')
    if (saved === 'en' || saved === 'ru') return saved
  } catch {
    // localStorage may be unavailable (privacy mode); fall through to the browser language.
  }
  return typeof navigator !== 'undefined' && navigator.language.toLowerCase().startsWith('ru') ? 'ru' : 'en'
}

export const i18n = createI18n({
  legacy: false,
  locale: detectLocale(),
  fallbackLocale: 'en',
  messages: { en, ru } satisfies Record<'en' | 'ru', MessageSchema>,
})

export function setLocale(locale: 'en' | 'ru') {
  i18n.global.locale.value = locale
  try {
    localStorage.setItem('encore.locale', locale)
  } catch {
    // ignore: persistence is a convenience
  }
}
