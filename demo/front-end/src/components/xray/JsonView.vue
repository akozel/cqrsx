<script setup lang="ts">
import { computed, ref } from 'vue'

const props = defineProps<{ value: unknown; changed?: string[]; expanded?: boolean; preview?: boolean }>()
const open = ref(props.expanded ?? false)

const text = computed(() => JSON.stringify(props.value, null, 2))
const line = computed(() => {
  const s = JSON.stringify(props.value)
  return s.length > 90 ? s.slice(0, 90) + '…' : s
})

/** Lines whose key path changed get a highlight: find keys at any depth that are the last path segment. */
const changedKeys = computed(() => new Set((props.changed ?? []).map((p) => p.split('.').pop() ?? p)))
const lines = computed(() =>
  text.value.split('\n').map((l) => {
    const m = /^\s*"([^"]+)":/.exec(l)
    return { l, hot: m ? changedKeys.value.has(m[1] ?? '') : false }
  }),
)
</script>

<template>
  <div class="font-mono text-xs">
    <button type="button" class="opacity-70 hover:opacity-100 text-left" @click="open = !open">
      <span class="mr-1">{{ open ? '▾' : '▸' }}</span><span v-if="!open" class="break-all">{{ line }}</span>
    </button>
    <pre v-if="open" class="mt-1 whitespace-pre-wrap break-all leading-snug"><span
        v-for="(row, i) in lines"
        :key="i"
        :class="{ 'json-hot': row.hot }"
      >{{ row.l }}
</span></pre>
  </div>
</template>
