<script setup lang="ts">
// A service boundary: a rounded frame with its name on the border. The projection frame dims
// while the read models are wiped.
import { computed, inject } from 'vue'
import type { NodeProps } from '@vue-flow/core'
import { useI18n } from 'vue-i18n'
import { PIPELINE } from '../context'
import type { PipeGroup } from '../graph'

const props = defineProps<NodeProps<PipeGroup>>()
const { t } = useI18n()
const ctx = inject(PIPELINE)!
const dimmed = computed(() => props.data.id === 'svc_projection' && ctx.wiped.value)
</script>

<template>
  <div class="pipe-service" :class="[`pipe-service-${data.tone}`, { 'pipe-dim': dimmed }]">
    <span class="pipe-service-title">{{ t(`pipeline.${data.label}`) }}</span>
  </div>
</template>
