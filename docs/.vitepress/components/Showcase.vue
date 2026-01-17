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
    code: `import { readCsv } from 'molniya';

// Blazing fast SIMD-accelerated reading
const { df } = await readCsv('bitcoin_7m_rows.csv', {
  delimiter: ',',
  hasHeader: true
});

console.log(df.shape); // [7381118, 8]
console.log(df.head(5));`,
    output: '✓ Loaded 7,381,118 rows in 1.28s\n[7381118, 8] DataFrame',
  },
  filtering: {
    code: `// Expressive filtering
const filtered = df
  .where(col => col("price").gt(50000))
  .select("timestamp", "price", "volume");

console.log(filtered.shape);
filtered.print();`,
    output:
      '✓ Filtered to 1,240,512 rows\n┌───────┬───────────┬──────────┐\n│ index │ price     │ volume   │\n├───────┼───────────┼──────────┤\n│ 0     │ 50421.12  │ 1.24     │\n└───────┴───────────┴──────────┘',
  },
  groupby: {
    code: `// SQL-like aggregations
const summary = df
  .groupby("category")
  .agg({
    price: "mean",
    quantity: "sum"
  });

summary.print();`,
    output:
      '┌──────────┬───────────┬──────────┐\n│ category │ price_avg │ qty_sum  │\n├──────────┼───────────┼──────────┤\n│ retail   │ 15.42     │ 1420     │\n└──────────┴───────────┴──────────┘',
  },
  types: {
    code: `// Full IDE support
const df = await readCsv<Schema>("data.csv");

// Error: Column 'non_existent' not found in Schema
df.col("non_existent").mean();

// Success: Auto-complete working
df.col("price").std();`,
    output: 'TS2345: Argument of type \'"non_existent"\' is not assignable...',
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
        <!-- Background Glow -->
        <div
            class="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[800px] h-[800px] bg-indigo-500/10 rounded-full blur-[120px] pointer-events-none">
        </div>

        <div class="max-w-7xl mx-auto relative z-10">

            <!-- Header -->
            <div class="text-center mb-20">
                <h2 class="text-3xl md:text-5xl font-bold dark:text-white mb-6 tracking-tight">Designed for Developers
                </h2>
                <p class="text-lg text-slate-500 dark:text-slate-400 max-w-2xl mx-auto">
                    A familiar API that feels like Pandas, powered by a high-performance engine designed for the
                    TypeScript ecosystem.
                </p>
            </div>

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
                                ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400 bg-indigo-500/5'
                                : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'">
                            {{ tab.label }}
                        </button>
                    </div>

                    <!-- Code Area -->
                    <div class="p-6 bg-[#0d1117] min-h-[300px]">
                        <pre
                            class="font-mono text-sm leading-relaxed overflow-x-auto"><code class="language-typescript" v-html="codeSamples[activeTab].code.replace(/</g, '&lt;').replace(/>/g, '&gt;')"></code></pre>
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

                    <!-- Benchmark Card -->
                    <div class="glass-card p-8 rounded-2xl relative overflow-hidden">
                        <div class="absolute top-0 right-0 p-4 opacity-10">
                            <Zap class="w-24 h-24" />
                        </div>
                        <h3 class="text-xl font-bold dark:text-white mb-6 flex items-center gap-2">
                            <Zap class="w-5 h-5 text-amber-500" />
                            Real-World Performance
                        </h3>
                        <p class="text-sm text-slate-500 mb-6">Loading 387MB (7.3M rows) Bitcoin CSV dataset.</p>

                        <div class="space-y-4">
                            <!-- Molniya -->
                            <div>
                                <div class="flex justify-between text-sm mb-1">
                                    <span class="font-semibold text-indigo-500">Molniya (Bun)</span>
                                    <span class="text-emerald-500 font-bold">1.28s</span>
                                </div>
                                <div class="h-2 bg-slate-100 dark:bg-slate-800 rounded-full overflow-hidden">
                                    <div
                                        class="h-full bg-emerald-500 w-[10%] rounded-full shadow-[0_0_10px_rgba(16,185,129,0.5)]">
                                    </div>
                                </div>
                            </div>

                            <!-- Arquero -->
                            <div>
                                <div class="flex justify-between text-sm mb-1">
                                    <span class="text-slate-500 dark:text-slate-400">Arquero (Node)</span>
                                    <span class="text-slate-500">~9.11.4s</span>
                                </div>
                                <div class="h-2 bg-slate-100 dark:bg-slate-800 rounded-full overflow-hidden">
                                    <div class="h-full bg-slate-400 w-[30%] rounded-full"></div>
                                </div>
                            </div>

                            <!-- Danfo -->
                            <div>
                                <div class="flex justify-between text-sm mb-1">
                                    <span class="text-slate-500 dark:text-slate-400">Danfo.js (Node)</span>
                                    <span class="text-slate-500">~70.1s</span>
                                </div>
                                <div class="h-2 bg-slate-100 dark:bg-slate-800 rounded-full overflow-hidden">
                                    <div class="h-full bg-slate-400 w-[95%] rounded-full"></div>
                                </div>
                            </div>
                        </div>
                        <p class="mt-6 text-[10px] text-slate-400 italic">
                            * Benchmarks performed on M1 MacBook Air. Arquero and Danfo run on Node.js using their
                            standard CSV utilities.
                        </p>
                    </div>

                    <!-- Coverage Card -->
                    <!-- <div class="glass-card p-8 rounded-2xl">
                        <h3 class="text-xl font-bold dark:text-white mb-6 flex items-center gap-2">
                            <Shield class="w-5 h-5 text-indigo-500" />
                            Almost Production Ready
                        </h3>
                        <div class="grid grid-cols-2 gap-4">
                            <div class="p-4 bg-indigo-500/5 border border-indigo-500/10 rounded-xl">
                                <div class="text-xs text-slate-500 mb-1">Tests</div>
                                <div class="text-xl font-bold text-indigo-400">140+</div>
                                <div class="text-[10px] text-slate-500">Fast regression checks</div>
                            </div>
                            <div class="p-4 bg-emerald-500/5 border border-emerald-500/10 rounded-xl">
                                <div class="text-xs text-slate-500 mb-1">Coverage</div>
                                <div class="text-xl font-bold text-emerald-400">95%</div>
                                <div class="text-[10px] text-slate-500">Strict safety verified</div>
                            </div>
                        </div>
                    </div> -->

                    <!-- Quick Install -->
                    <div
                        class="glass-card p-6 rounded-2xl flex items-center justify-between gap-4 border-l-4 border-l-indigo-500">
                        <div class="font-mono text-sm dark:text-slate-200">
                            <span class="text-indigo-500 mr-2">$</span>bun add molniya
                        </div>
                        <button @click="copyInstall"
                            class="p-2 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg transition-colors text-slate-500 hover:text-indigo-500 relative group"
                            title="Copy to clipboard">
                            <span v-if="copied"
                                class="absolute -top-8 left-1/2 -translate-x-1/2 bg-gray-900 text-white text-xs py-1 px-2 rounded shadow-lg animate-fade-in-up">Copied!</span>
                            <Check v-if="copied" class="w-5 h-5 text-emerald-500" />
                            <Copy v-else class="w-5 h-5" />
                        </button>
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
