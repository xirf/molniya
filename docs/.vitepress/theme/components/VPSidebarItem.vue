<script setup lang="ts">
import type { DefaultTheme } from 'vitepress/theme'
import { computed } from 'vue'
// @ts-expect-error - It's not a type error, it's a vitepress internal module
import { useSidebarControl } from 'vitepress/dist/client/theme-default/composables/sidebar.js'
import VPLink from 'vitepress/dist/client/theme-default/components/VPLink.vue'

const props = defineProps<{
  item: DefaultTheme.SidebarItem & {
    icon?: string
  }
  depth: number
}>()

const {
  collapsed,
  collapsible,
  isLink,
  isActiveLink,
  hasActiveLink,
  hasChildren,
  toggle
} = useSidebarControl(computed(() => props.item))

const sectionTag = computed(() => (hasChildren.value ? 'section' : `div`))

const linkTag = computed(() => (isLink.value ? 'a' : 'div'))

const textTag = computed(() => {
  return !hasChildren.value
    ? 'p'
    : props.depth + 2 === 7
      ? 'p'
      : `h${props.depth + 2}`
})

const itemRole = computed(() => (isLink.value ? undefined : 'button'))

const classes = computed(() => [
  [`level-${props.depth}`],
  { collapsible: collapsible.value },
  { collapsed: collapsed.value },
  { 'is-link': isLink.value },
  { 'is-active': isActiveLink.value },
  { 'has-active': hasActiveLink.value }
])

function onItemInteraction(e: MouseEvent | Event) {
  if ('key' in e && e.key !== 'Enter') {
    return
  }
  !props.item.link && toggle()
}

function onCaretClick() {
  props.item.link && toggle()
}
</script>

<template>
  <component :is="sectionTag" :class="classes">
    <div v-if="item.text" :role="itemRole" v-on="item.items
      ? { click: onItemInteraction, keydown: onItemInteraction }
      : {}
      " :tabindex="item.items && 0"
      class="flex items-center justify-between py-1.5 px-3 my-0.5 rounded-lg transition-colors duration-200 select-none cursor-pointer relative"
      :class="[
        isActiveLink
          ? 'bg-[var(--vp-c-brand-soft)] text-[var(--vp-c-brand)] font-medium active'
          : 'text-[var(--vp-c-text-2)] hover:text-[var(--vp-c-text-1)] hover:bg-[var(--vp-c-brand-soft)]/50',
      ]">
      <VPLink v-if="item.link" :tag="linkTag" class="link relative flex items-center flex-grow truncate min-w-0 pl-7"
        :href="item.link" :rel="item.rel" :target="item.target">
        <span v-if="item.icon" :class="[item.icon, 'mr-2 size-5 shrink-0 inline-block']"></span>
        <component :is="textTag" class="text truncate" v-html="item.text" />
      </VPLink>

      <div v-else class="text-wrapper flex items-center flex-grow truncate min-w-0">
        <span v-if="item.icon" :class="[item.icon, 'mr-2 size-5 shrink-0 inline-block']"></span>
        <component :is="textTag" class="text truncate" v-html="item.text" />
      </div>

      <div v-if="item.collapsed != null && item.items && item.items.length"
        class="caret flex items-center justify-center size-6 shrink-0 text-zinc-400 hover:text-zinc-200 transition-colors"
        role="button" aria-label="toggle section" @click.stop="onCaretClick" @keydown.enter="onCaretClick" tabindex="0">
        <span class="i-solar:alt-arrow-right-linear transition-transform duration-200"
          :class="{ 'rotate-90': !item.collapsed }" />
      </div>
    </div>

    <div v-if="item.items && item.items.length" class="items relative">
      <div class="decoration" />
      <template v-if="depth < 5">
        <VPSidebarItem v-for="i in item.items" :key="i.text" :item="i" :depth="depth + 1" />
      </template>
    </div>
  </component>
</template>

<style scoped>
.level-0 {
  position: relative;
}

.level-0 .decoration {
  content: '';
  position: absolute;
  left: 1.4rem;
  top: 50%;
  transform: translateY(-50%);
  width: 1px;
  height: 100%;
  background-color: var(--vp-c-border);
  opacity: 0.1;
}

.active .link::before {
  content: '';
  position: absolute;
  left: 0.6rem;
  top: 50%;
  transform: translateY(-50%);
  width: 2px;
  height: 100%;
  background-color: var(--vp-c-brand);
}
</style>
