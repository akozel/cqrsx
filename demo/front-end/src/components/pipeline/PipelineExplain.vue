<script setup lang="ts">
// The details behind a block of the diagram: what it is, what it does in the demo and, for the
// aggregates, the table of rules. Texts live under `explain.<id>` in the i18n files.
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { onKeyStroke } from '@vueuse/core'
import { useSessionStore } from '@/stores/session'
import { toneOf, type PipeGroupId, type PipeNodeId } from './graph'

const props = defineProps<{ id: PipeNodeId | PipeGroupId | null }>()
const emit = defineEmits<{ close: [] }>()
const { t, tm, rt } = useI18n()
const session = useSessionStore()

interface Rule {
  code: string
  when: string
  check: string
}
/** A message list under `explain.<id>.<key>`, or nothing when the block has none. */
function list<T>(key: string): T[] {
  const value = props.id ? tm(`explain.${props.id}.${key}`) : null
  return Array.isArray(value) ? (value as T[]) : []
}
const points = computed(() => list<string>('points').map((m) => rt(m)))
const rules = computed(() =>
  list<Rule>('rules').map((r) => ({ code: rt(r.code), when: rt(r.when), check: rt(r.check) })),
)
const tone = computed(() => (props.id ? toneOf(props.id) : 'cyan'))

onKeyStroke('Escape', () => {
  if (props.id) emit('close')
})
</script>

<template>
  <Transition name="fade">
    <div
      v-if="id"
      class="fixed inset-0 z-50 flex items-center justify-center bg-bg/70 p-4"
      @click.self="emit('close')"
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="pipe-explain-title"
        class="pipe-explain w-[40rem] max-w-full max-h-[90vh] overflow-y-auto rounded-xl border bg-panel p-5 shadow-2xl text-sm"
        :class="`pipe-explain-${tone}`"
      >
        <div class="mb-2 flex items-start gap-2">
          <h2 id="pipe-explain-title" class="pipe-explain-title text-base font-semibold">
            {{ t(`explain.${id}.title`) }}
          </h2>
          <button
            type="button"
            class="ml-auto text-xs opacity-70 hover:opacity-100"
            :aria-label="t('common.close')"
            @click="emit('close')"
          >
            ✕
          </button>
        </div>
        <p class="leading-relaxed opacity-90">{{ t(`explain.${id}.lead`) }}</p>
        <div v-if="id === 'user'" class="mt-3 flex flex-wrap gap-2">
          <button
            v-for="p in session.demo.personas"
            :key="p.fan_id"
            type="button"
            class="pipe-persona flex items-center gap-2 rounded-lg border px-2 py-1 text-sm"
            :class="
              session.persona === p.fan_id
                ? 'border-neon-cyan bg-neon-cyan/10'
                : 'border-border hover:border-neon-cyan/60'
            "
            @click="session.persona = p.fan_id"
          >
            <span class="text-xl">{{ p.avatar }}</span>
            <span class="font-semibold">{{ p.name }}</span>
          </button>
        </div>
        <table v-if="rules.length" class="pipe-rules mt-3 w-full text-xs">
          <thead>
            <tr>
              <th>{{ t('explain._head.code') }}</th>
              <th>{{ t('explain._head.when') }}</th>
              <th>{{ t('explain._head.check') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="r in rules" :key="r.code">
              <td class="font-mono whitespace-nowrap">{{ r.code }}</td>
              <td class="font-mono">{{ r.when }}</td>
              <td>{{ r.check }}</td>
            </tr>
          </tbody>
        </table>
        <ul v-if="points.length" class="mt-3 list-disc space-y-1 pl-5 leading-relaxed opacity-90">
          <li v-for="(p, i) in points" :key="i">{{ p }}</li>
        </ul>
        <div class="mt-4 flex justify-end">
          <button
            type="button"
            class="rounded border border-border px-3 py-1 text-xs"
            autofocus
            @click="emit('close')"
          >
            {{ t('common.close') }}
          </button>
        </div>
      </div>
    </div>
  </Transition>
</template>
