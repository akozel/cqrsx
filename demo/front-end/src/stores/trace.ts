import { defineStore } from 'pinia'
import { ref } from 'vue'

/** [P1] Hovering an event highlights its task(s) and document(s) through the shared correlation_id. */
export const useTraceStore = defineStore('trace', () => {
  const correlationId = ref<string | null>(null)
  const aggregateId = ref<string | null>(null)
  function set(correlation: string | undefined, aggregate: string | undefined) {
    correlationId.value = correlation ?? null
    aggregateId.value = aggregate ?? null
  }
  function clear() {
    correlationId.value = null
    aggregateId.value = null
  }
  return { correlationId, aggregateId, set, clear }
})
