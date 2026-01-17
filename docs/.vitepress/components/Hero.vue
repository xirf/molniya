<template>
    <Ray class="h-[60vh] -top-16 pointer-events-none opacity-[.35] dark:opacity-40" />
    <section
        class="z-10 relative flex w-full pt-6 md:pt-0 mb-16 md:mb-8 px-6 min-h-[80svh] items-center justify-center">
        <div class="flex flex-col text-center">
            <div class="flex flex-col gap-4 max-w-screen-lg items-center">
                <h1 class="text-6xl xl:text-9xl font-black font-mono xl:mb-10 tracking-tighter">Molniya</h1>
                <p class="text-4xl xl:text-5xl mb-6 font-bold tracking-tight">Ergonomic data analysis for <span
                        class="bg-gradient-to-r from-blue-400 to-indigo-400 bg-clip-text text-transparent">TypeScript.</span>
                </p>
                <p
                    class="text-xl md:text-2xl text-slate-500 dark:text-slate-400 leading-normal text-left w-full max-w-2xl md:text-center">
                    A simple, typed DataFrame library. In-memory, zero dependencies, and blazing fast with SIMD line
                    finding.</p>
                <section
                    class="flex flex-col sm:flex-row items-start sm:items-center w-full md:w-auto gap-4 mt-8 mb-12">
                    <a class="text-white font-semibold text-lg bg-indigo-500 dark:bg-indigo-600 px-8 py-3 rounded-full hover:scale-105 active:scale-95 transition-transform"
                        id="hero-get-started" href="/guide/getting-started">
                        Get Started
                    </a>
                    <div class="relative flex flex-1 gap-3 text-indigo-500">
                        <code
                            class="text-indigo-500 dark:text-indigo-400 font-mono font-medium text-lg bg-indigo-50/50 dark:bg-indigo-500/10 px-6 py-2.5 rounded-full border border-indigo-200/50 dark:border-indigo-500/20">
                            bun add molniya
                        </code>
                    </div>
                </section>
            </div>
        </div>
    </section>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue';

import Ray from './Ray.vue';

const copied = ref(false);
watch(copied, (value) => {
  // [INFO] navigator.clipboard available only in secure contexts.
  if (value && window.isSecureContext) {
    navigator.clipboard.writeText('bun add molniya');

    setTimeout(() => {
      copied.value = false;
    }, 2000);
  }
});
</script>
