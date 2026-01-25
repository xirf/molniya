<script setup lang="ts">
import { Check, ChevronRight, Copy, Shield, Terminal, Zap } from 'lucide-vue-next';
import { ref } from 'vue';

const activeTab = ref<SampleKey>('csv');
const copied = ref(false);

const tabs = [
  { id: 'csv', label: 'CSV Loading' },
  { id: 'filtering', label: 'Filtering' },
  { id: 'groupby', label: 'GroupBy' },
  { id: 'types', label: 'Type-Safety' },
];

const codeSamples = {
  csv: {
    output:
      '   timestamp  |      price |     volume\n---------------------------------------\n 167253120000 |   50421.12 |   1.245000\n 167253120060 |   50422.50 |   0.052000\n... (7381116 more rows)',
  },
  filtering: {
    output: '   item |      price\n--------------------\n    GPU |    1200.00',
  },
  groupby: {
    output:
      '   dept | avg_salary\n--------------------\n  Sales |   62500.00\n    Eng |   92500.00\n     HR |   55000.00',
  },
  types: {
    output: "Error: Column 'non_existent' not found",
  },
} as const;

type SampleKey = keyof typeof codeSamples;

const copyInstall = () => {
  navigator.clipboard.writeText('bun add molniya');
  copied.value = true;
  setTimeout(() => {
    copied.value = false;
  }, 2000);
};
</script>

<template>
    <section class="py-24 px-6 relative overflow-hidden">
        <div class="max-w-7xl mx-auto relative z-10">
            <div class="grid lg:grid-cols-2 gap-12 items-start opacity-90">

                <!-- Left Column: IDE Showcase -->
                <div
                    class="glass-card rounded-2xl overflow-hidden border border-slate-200 dark:border-slate-800 shadow-2xl">
                    <!-- Window Controls -->
                    <div
                        class="bg-slate-100 dark:bg-slate-900/50 px-4 py-3 flex items-center justify-between border-b border-slate-200 dark:border-slate-800">
                        <div class="flex gap-2">
                            <div class="w-3 h-3 rounded-full bg-red-400"></div>
                            <div class="w-3 h-3 rounded-full bg-amber-400"></div>
                            <div class="w-3 h-3 rounded-full bg-emerald-400"></div>
                        </div>
                        <div class="text-xs font-mono text-slate-400">analysis.ts</div>
                        <div class="w-10"></div> <!-- Spacer -->
                    </div>

                    <!-- Tabs -->
                    <div
                        class="flex border-b border-slate-200 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-900/30 overflow-x-auto">
                        <button v-for="tab in tabs" :key="tab.id" @click="activeTab = tab.id as SampleKey"
                            class="px-4 py-3 text-sm font-medium transition-colors border-b-2 whitespace-nowrap"
                            :class="activeTab === tab.id
                                ? 'border-cat-yellow text-cat-yellow bg-cat-yellow/5'
                                : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'">
                            {{ tab.label }}
                        </button>
                    </div>

                    <!-- Code Area -->
                    <div class="code-area p-6 bg-[#0d1117] min-h-[300px] text-sm leading-relaxed overflow-x-auto">
                        <slot :name="activeTab"></slot>
                    </div>
                    <!-- Output Area -->
                    <div class="bg-[#090c10] border-t border-slate-800 p-4 font-mono text-sm">
                        <div class="flex items-center gap-2 text-slate-500 mb-2 text-xs uppercase tracking-wider">
                            <Terminal class="w-3 h-3" /> Terminal
                        </div>
                        <div class="text-emerald-400 whitespace-pre-wrap">{{ codeSamples[activeTab].output }}</div>
                    </div>
                </div>

                <!-- Right Column: Stats & Benchmarks -->
                <div class="space-y-8">

                    <!-- Header -->
                    <div class="mb-20">
                        <h2 class="text-3xl md:text-5xl font-bold dark:text-white mb-6 tracking-tight leading-tight">
                            <span class="text-cat-yellow italic">Process</span> with
                            <br />
                            <span class="text-violet-400 italic">confidence</span>
                        </h2>
                        <div class="flex flex-col gap-4">
                            <p class="text-lg text-slate-500 dark:text-slate-400 max-w-2xl mx-auto leading-relaxed">
                                Molniya aims to process large datasets efficiently without sacrificing predictability.
                                We're building toward a library where performance, memory efficiency, and type safety
                                work together, not against each other.
                            </p>
                            <p class="text-lg text-slate-500 dark:text-slate-400 max-w-2xl mx-auto leading-relaxed">
                                We're working toward full type inference so TypeScript knows your data shape throughout
                                your pipeline. Filter, group, and aggregate with confidence and autocomplete.
                            </p>
                        </div>
                    </div>

                </div>
            </div>
        </div>
    </section>
</template>

<style scoped>
.mask-gradient {
    mask-image: linear-gradient(to bottom, black 50%, transparent 100%);
}

:deep(.language-typescript) {
    color: #e2e8f0;
}
</style>
