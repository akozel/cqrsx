<script setup lang="ts">
// An actor outside the code: a diamond for a screen or a mailbox, a circle for the person. The person
// wears the selected persona: its avatar and name.
import { computed, inject } from 'vue'
import { Handle, type NodeProps } from '@vue-flow/core'
import { useI18n } from 'vue-i18n'
import { PIPELINE } from '../context'
import { SIDE_POSITION, type PipeNode, type Side } from '../graph'

const props = defineProps<NodeProps<PipeNode>>()
const { t } = useI18n()
const ctx = inject(PIPELINE)!
const SIDES: Side[] = ['top', 'right', 'bottom', 'left']
const glow = computed(() => ctx.choreo.glow[props.data.id])
const isUser = computed(() => props.data.id === 'user')
const icon = computed(() => (isUser.value ? ctx.persona.value.avatar : props.data.icon))
const label = computed(() => (isUser.value ? ctx.persona.value.name : t(`pipeline.${props.data.label}`)))
</script>

<template>
  <div class="pipe-actor">
    <Handle v-for="side in SIDES" :id="side" :key="side" type="source" :position="SIDE_POSITION[side]" />
    <span
      class="pipe-actor-shape"
      :class="[`pipe-glow-${glow}`, { 'pipe-person-shape': data.kind === 'person' }]"
    />
    <span class="pipe-actor-label"
      ><span class="pipe-actor-icon">{{ icon }}</span
      >{{ label }}</span
    >
  </div>
</template>
