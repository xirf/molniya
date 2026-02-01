<script setup lang="ts">
import { useData } from 'vitepress'
import { nextTick } from 'vue'

const { isDark } = useData()

// VitePress uses the 'appearance' option to handle the toggle logic,
// but we can just simplify it by interacting with isDark directly.
// The default VPSwitchAppearance also handles view transitions if enabled.

function toggleAppearance(event: MouseEvent) {
  const isAppearanceTransition = document.startViewTransition() && !window.matchMedia('(prefers-reduced-motion: reduce)').matches

  if (!isAppearanceTransition || !event) {
    isDark.value = !isDark.value
    return
  }

  const x = event.clientX
  const y = event.clientY
  const endRadius = Math.hypot(
    Math.max(x, innerWidth - x),
    Math.max(y, innerHeight - y)
  )

  const transition = document.startViewTransition(async () => {
    isDark.value = !isDark.value
    await nextTick()
  })

  const clipPath = [
    `circle(0px at ${x}px ${y}px)`,
    `circle(${Math.hypot(
      Math.max(x, innerWidth - x),
      Math.max(y, innerHeight - y),
    )}px at ${x}px ${y}px)`,
  ];

  transition.ready.then(() => {
    document.documentElement.animate(
      { clipPath: isDark.value ? clipPath.reverse() : clipPath },
      {
        duration: 300,
        easing: 'ease-in',
        fill: 'forwards',
        pseudoElement: `::view-transition-${isDark.value ? 'old' : 'new'}(root)`,
      },
    );
  })
}
</script>

<template>
  <button class="VPSwitchAppearance" type="button" role="switch" aria-label="Toggle dark mode" :aria-checked="isDark"
    @click="toggleAppearance">
    <div v-if="!isDark" class="i-solar:sun-2-bold icon-sun"></div>
    <div v-else class="i-solar:moon-stars-bold icon-moon"></div>
  </button>
</template>

<style scoped>
.VPSwitchAppearance {
  display: flex;
  justify-content: center;
  align-items: center;
  width: 36px;
  height: 36px;
  border-radius: 50%;
  color: var(--vp-c-text-2);
  transition: color 0.5s, background-color 0.5s;
}

.VPSwitchAppearance:hover {
  color: var(--vp-c-text-1);
}

.icon-sun,
.icon-moon {
  width: 22px;
  height: 22px;
}
</style>
