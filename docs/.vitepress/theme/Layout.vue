<script setup lang="ts">
import DefaultTheme from 'vitepress/theme'
import PageActionMenu from './components/PageActionMenu.vue'
import SidebarText from './components/SidebarText.vue';
import Ray from './components/Ray.vue';

const { Layout } = DefaultTheme
</script>

<template>
  <Layout>
    <template #doc-top>
      <Ray class="h-[220px] top-0 left-0 opacity-25 dark:opacity-[.55] pointer-events-none" static />
    </template>
    <template #doc-before>
      <div class="flex justify-between mb-2">
        <SidebarText />
        <PageActionMenu />
      </div>
    </template>
  </Layout>
</template>

<style>
/* Hide the default outline */
.VPDocAsideOutline {
  display: none !important;
}
</style>
