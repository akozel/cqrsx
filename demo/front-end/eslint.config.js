import pluginVue from 'eslint-plugin-vue'
import { defineConfigWithVueTs, vueTsConfigs } from '@vue/eslint-config-typescript'
import skipFormatting from '@vue/eslint-config-prettier/skip-formatting'

export default defineConfigWithVueTs(
  { name: 'app/files-to-lint', files: ['**/*.{ts,mts,tsx,vue}'] },
  { name: 'app/files-to-ignore', ignores: ['**/dist/**', '**/node_modules/**', '**/coverage/**'] },
  pluginVue.configs['flat/essential'],
  vueTsConfigs.recommended,
  skipFormatting,
  // The small UI primitives (Toasts, Dice, Countdown, Tooltip…) keep their one-word names (DESIGN.md §4.2).
  { name: 'app/ui-primitives', files: ['src/components/ui/*.vue'], rules: { 'vue/multi-word-component-names': 'off' } },
)
