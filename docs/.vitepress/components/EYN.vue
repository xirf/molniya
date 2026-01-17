<script setup lang="ts">
import { ChartSpline, Code2, Cpu, Shield, Table, Zap } from 'lucide-vue-next';
</script>

<template>
    <!-- Features Grid (Bento) -->
    <section id="features" class="py-20 px-6 relative">
        <div class="max-w-7xl mx-auto">
            <div class="text-center mb-16 flex flex-col items-center">
                <h2 class="text-3xl md:text-5xl font-bold dark:text-white mb-4 tracking-tight">Everything you need</h2>
                <p class="text-lg max-w-2xl text-slate-500 dark:text-slate-400 mx-auto">
                    A complete toolset for data manipulation and analysis, built for the modern TypeScript ecosystem.
                </p>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 auto-rows-[minmax(180px,auto)]">

                <!-- Main Feature: Type Safety -->
                <div class="glass-card rounded-2xl p-8 lg:col-span-2 row-span-2 relative overflow-hidden group">
                    <div
                        class="absolute top-0 right-0 w-64 h-64 bg-indigo-500/10 rounded-full blur-3xl -mr-16 -mt-16 transition-opacity group-hover:opacity-100 opacity-50">
                    </div>
                    <div
                        class="w-12 h-12 rounded-lg bg-slate-100 dark:bg-slate-800 flex items-center justify-center mb-6 text-indigo-500">
                        <Shield />
                    </div>
                    <h3 class="text-2xl font-bold text-gray-900 dark:text-white mb-2">Bulletproof Type Safety</h3>
                    <p class="text-slate-600 dark:text-slate-400 mb-6">
                        Molniya leverages TypeScript's advanced type system to infer schemas directly from your data.
                        Get full autocomplete and compile-time checks for your column names and types.
                    </p>
                    <div
                        class="bg-gray-100 dark:bg-slate-800/50 rounded-lg p-3 font-mono text-xs text-slate-600 dark:text-slate-300 border border-slate-200 dark:border-slate-700/50">
                        <span class="text-indigo-600 dark:text-indigo-400">const</span> df = <span
                            class="text-yellow-600 dark:text-yellow-400">readCsv</span>(<span
                            class="text-green-600 dark:text-green-400">"data.csv"</span>);<br />
                        <span class="text-slate-400">// df.col("invalid") ➔ Error!</span><br />
                        <span class="text-slate-400">// df.col("price").sum() ➔ Correctly typed</span>
                    </div>
                </div>

                <!-- Feature: SIMD Parsing -->
                <div class="glass-card rounded-2xl p-6 lg:col-span-1">
                    <div
                        class="w-10 h-10 rounded-lg bg-slate-100 dark:bg-slate-800 flex items-center justify-center mb-4 text-emerald-400">
                        <Cpu />
                    </div>
                    <h3 class="text-xl font-bold text-gray-900 dark:text-white mb-2">Blazing Fast I/O</h3>
                    <p class="text-sm text-slate-600 dark:text-slate-400">
                        SIMD-accelerated line finding and direct byte parsing. 7.3M rows in <span
                            class="text-emerald-500 font-bold">1.2s</span>.
                    </p>
                </div>

                <!-- Feature: Familiar API -->
                <div class="glass-card rounded-2xl p-6 lg:col-span-1">
                    <div
                        class="w-10 h-10 rounded-lg bg-slate-100 dark:bg-slate-800 flex items-center justify-center mb-4 text-purple-400">
                        <Zap />
                    </div>
                    <h3 class="text-xl font-bold text-gray-900 dark:text-white mb-2">Familiar API</h3>
                    <p class="text-sm text-slate-600 dark:text-slate-400">
                        Pandas/Polars inspired syntax. <code class="text-purple-500">filter</code>, <code
                            class="text-purple-500">groupby</code>, and <code class="text-purple-500">select</code> just
                        work.
                    </p>
                </div>

                <!-- Feature: Lazy Loading -->
                <div class="glass-card rounded-2xl p-6 lg:col-span-1">
                    <div
                        class="w-10 h-10 rounded-lg bg-slate-100 dark:bg-slate-800 flex items-center justify-center mb-4 text-sky-400">
                        <Table />
                    </div>
                    <h3 class="text-xl font-bold text-gray-900 dark:text-white mb-2">Lazy Frames</h3>
                    <p class="text-sm text-slate-600 dark:text-slate-400">
                        Work with datasets larger than RAM. Process data in streams without loading the whole file.
                    </p>
                </div>

                <!-- Feature: Zero Dependencies -->
                <div class="glass-card rounded-2xl p-6 lg:col-span-1">
                    <div
                        class="w-10 h-10 rounded-lg bg-slate-100 dark:bg-slate-800 flex items-center justify-center mb-4 text-amber-400">
                        <Code2 />
                    </div>
                    <h3 class="text-xl font-bold text-gray-900 dark:text-white mb-2">Zero Deps</h3>
                    <p class="text-sm text-slate-600 dark:text-slate-400">
                        Lightweight and portable. No heavy C++ binaries or complex native dependencies.
                    </p>
                </div>

                <!-- Feature: Aggregations -->
                <div
                    class="glass-card rounded-2xl p-8 lg:col-span-4 bg-gradient-to-br from-gray-50 dark:from-slate-900 to-indigo-50/50 dark:to-indigo-950/30">
                    <div class="flex flex-col md:flex-row gap-6 items-center">
                        <div>
                            <div
                                class="w-12 h-12 rounded-lg bg-slate-100 dark:bg-slate-800 flex items-center justify-center mb-6 text-indigo-400">
                                <ChartSpline />
                            </div>
                            <h3 class="text-2xl font-bold text-gray-900 dark:text-white mb-2">Advanced Aggregations</h3>
                            <p class="text-slate-600 dark:text-slate-400 max-w-xl">
                                Robust GroupBy engine with support for <code
                                    class="font-mono bg-gray-200 dark:bg-slate-800 px-1 rounded">sum</code>, <code
                                    class="font-mono bg-gray-200 dark:bg-slate-800 px-1 rounded">mean</code>, <code
                                    class="font-mono bg-gray-200 dark:bg-slate-800 px-1 rounded">std</code>, and custom
                                collectors.
                            </p>
                        </div>
                        <div
                            class="flex-1 w-full bg-white dark:bg-[#0d1117] rounded-lg p-4 border border-slate-200 dark:border-slate-800 font-mono text-xs text-slate-600 dark:text-slate-300">
                            <div class="mb-2 text-slate-400 dark:text-slate-500">// Grouping made easy</div>
                            <div class="text-indigo-500 dark:text-indigo-300">
                                df.groupby(<span class="text-green-500">"category"</span>).agg({ <br />
                                &nbsp;&nbsp;<span class="text-green-500">"price"</span>: <span
                                    class="text-green-500">"mean"</span>, <br />
                                &nbsp;&nbsp;<span class="text-green-500">"quantity"</span>: <span
                                    class="text-green-500">"sum"</span> <br />
                                });
                            </div>
                        </div>
                    </div>
                </div>

            </div>
        </div>
    </section>
</template>

<style>
@reference "../theme/custom.css";
</style>
