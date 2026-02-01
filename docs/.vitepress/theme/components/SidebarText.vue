<script setup lang="ts">
import { useActiveSidebar } from '../composables/useActiveSidebar'

const activeItem = useActiveSidebar()
</script>

<template>
  <p class="active-sidebar-text items-center hidden md:flex text-[var(--vp-c-text-1)]" v-if="activeItem">
    <span v-if="activeItem.icon" role="decorative" class="mr-2">
      <div :class="[activeItem.icon, 'inline-block']" />
    </span>
    <span class="leading-none mb-1 font-mono" v-html="activeItem.text"></span>
  </p>
</template>

<style scoped>
.active-sidebar-text {
  font-weight: 500;
  color: var(--vp-c-brand-1);
}
</style>
