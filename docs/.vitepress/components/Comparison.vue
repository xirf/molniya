<script setup lang="ts">
import { GlassWaterIcon, SkullIcon } from 'lucide-vue-next';
</script>

<template>
    <!-- Benchmarks Section -->
    <section id="benchmarks" class="py-24 bg-cat-base relative">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-16 items-center">

                <!-- Chart -->
                <div>
                    <h2 class="text-3xl font-bold text-white mb-2">Speed vs Stability</h2>
                    <p class="text-slate-500 mb-8">
                        Time to filter & aggregate a <span class="text-slate-400 font-semibold">6.7GB Dataset</span>.
                        <br>Most JS libraries crash. Molniya thrives.
                    </p>

                    <div class="space-y-6 font-mono text-sm">
                        <!-- Molniya -->
                        <div>
                            <div class="flex justify-between mb-2">
                                <span class="flex items-center gap-2">
                                    <span class="text-white font-bold">Molniya (Stream)</span>
                                </span>
                                <span class="text-yellow-100">59.4s</span>
                            </div>
                            <div class="w-full bg-gray-300/10 rounded-full h-3">
                                <div class="bg-yellow-100 h-3 rounded-full relative" style="width: 85%">
                                    <div
                                        class="absolute -right-1 -top-1 w-5 h-5 bg-yellow-100/20 rounded-full animate-pulse">
                                    </div>
                                </div>
                            </div>
                        </div>

                        <!-- Pandas -->
                        <div>
                            <div class="flex justify-between mb-2">
                                <span class="text-gray-400">Pandas (Python)</span>
                                <span class="text-gray-400">64.0s</span>
                            </div>
                            <div class="w-full bg-gray-300/10 rounded-full h-3">
                                <div class="bg-sky-700 h-3 rounded-full opacity-60" style="width: 92%"></div>
                            </div>
                        </div>

                        <!-- Polars -->
                        <div>
                            <div class="flex justify-between mb-2">
                                <span class="text-gray-500">Polars (Rust)</span>
                                <span class="text-gray-500">3.4s (High RAM)</span>
                            </div>
                            <div class="w-full bg-gray-300/10 rounded-full h-3">
                                <div class="bg-orange-400 h-3 rounded-full opacity-40" style="width: 5%"></div>
                            </div>
                        </div>

                        <!-- Crashed Libraries -->
                        <div class="pt-4 border-t border-gray-400/10 mt-4">
                            <div class="flex justify-between items-center opacity-50">
                                <span>Danfo.js / Arquero</span>
                                <span class="flex items-center gap-2 text-red-300">
                                    <SkullIcon />
                                    OOM Crash
                                </span>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Feature Description -->
                <div class="bg-cat-mantle p-8 rounded-2xl border border-gray-400/10 shadow-2xl">
                    <h3 class="text-xl font-bold! text-white mb-6 flex items-center gap-3">
                        <GlassWaterIcon class="text-sky-400" />
                        Drink with a Straw.
                    </h3>
                    <p class="text-gray-400 mb-6 leading-relaxed">
                        Don't swallow the ocean. Standard libraries try to load the entire 8GB file into RAM, killing
                        your process immediately.
                    </p>
                    <p class="text-gray-400 mb-8 leading-relaxed">
                        Molniya's <code class="bg-violet-400/10 px-2 py-1 rounded text-violet-400">scanCsv</code>
                        creates a
                        <span class="text-green">zero-copy stream</span>, allowing you to process terabytes of data
                        on a micro-instance without ever hitting the memory ceiling.
                    </p>

                    <div class="grid grid-cols-2 gap-4">
                        <div class="bg-cat-base p-4 rounded-lg border border-gray-400/10">
                            <div class="text-blue font-bold mb-1">Standard</div>
                            <div class="text-xs text-red">RAM = Dataset Size</div>
                            <div class="mt-2 h-1 w-full bg-cat-surface0 rounded overflow-hidden">
                                <div class="h-full bg-red w-full"></div>
                            </div>
                        </div>
                        <div class="bg-cat-base p-4 rounded-lg border border-gray-400/10">
                            <div class="text-green font-bold mb-1">Molniya</div>
                            <div class="text-xs text-green">RAM = Dataset Size</div>
                            <div class="mt-2 h-1 w-full bg-cat-surface0 rounded overflow-hidden">
                                <div class="h-full bg-green w-[10%]"></div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </section>
</template>