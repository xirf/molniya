<template>
  <div class="-mt-16">
    <Ray class="h-[60vh] pointer-events-none absolute top-0 opacity-[.35] dark:opacity-50" />
    <section class="relative  h-screen flex items-center justify-center pt-16 overflow-hidden min-h-[800px]">
      <div
        class="absolute top-1/2 left-1/2 transform w-[max(600px,60vw)] aspect-square -translate-x-1/2 -translate-y-1/2 opacity-20 select-none pointer-events-none decor">
        <div class="w-full h-full relative">
          <div class="abs-center size-full overflow-visible ss rounded-full"></div>
        </div>
      </div>
      <div
        class="absolute top-1/2 left-1/2 transform w-[max(400px,30vw)] aspect-square -translate-x-1/2 -translate-y-1/2 opacity-80 select-none pointer-events-none decor rev">
        <div class="w-full h-full relative">
          <div class="abs-center size-full overflow-visible ss rounded-full"></div>
        </div>
      </div>
      <div class="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center z-10">
        <h1 class="text-5xl md:text-7xl font-bold tracking-tight text-gray-600 dark:text-white mb-6">
          Process datasets larger than memory in
          <span class="text-transparent bg-clip-text bg-gradient-to-r from-yellow to-orange">JavaScript.</span>
        </h1>

        <p class="mt-4 max-w-2xl mx-auto text-xl">
          A streaming DataFrame library built for Bun with type safety in mind.
        </p>

        <p class="text my-2 text-gray-400">
          This is experimental library, APIs are not stable and breaking changes may occur at any time.
        </p>

        <section class="flex items-center justify-center my8">
          <div class="flex items-center gap-4 flex-wrap flex-col lg:flex-row">
            <a class="text-white font-semibold text-lg bg-indigo-500 dark:bg-indigo-600 px-8 py-2 rounded-full active:scale-95 transition-transform"
              id="hero-get-started" href="/guide/getting-started">
              Get Started
            </a>
            <div class="relative flex flex-1 gap-3 text-indigo-500 shrink-0">
              <button @click="copyToClipboard('bun add molniya')"
                class="group text-indigo-500 dark:text-indigo-400 font-mono flex items-center gap-3 shrink-0 font-medium text-lg bg-indigo-50/50 dark:bg-indigo-500/10 px-6 py-2 rounded-full border border-indigo-200/50 dark:border-indigo-500/20 hover:bg-indigo-100/50 dark:hover:bg-indigo-500/20 transition-all cursor-pointer"
                title="Copy to clipboard">
                <span>bun add molniya</span>
                <div class="size-5 transition-transform duration-200"
                  :class="copied ? 'i-solar:check-circle-linear !text-green-500 scale-110' : 'i-solar:copy-linear group-hover:scale-110'" />
              </button>
            </div>
          </div>
        </section>
      </div>
    </section>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import Ray from './Ray.vue'
const copied = ref(false)

function copyToClipboard(text) {
  copied.value = true
  navigator.clipboard.writeText(text)
  setTimeout(() => {
    copied.value = false
  }, 2000)
}
</script>

<style scoped>
.abs-center {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}

.ss {
  background-image: url('data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" width="1384" height="1363" viewBox="0 0 1384 1363" fill="none"><path d="M811.727 1362.41L810.742 1362.59L795.96 1278.75C796.288 1278.7 796.616 1278.64 796.944 1278.58L811.727 1362.41Z" fill="%2390A4AE"/><path d="M586.309 1278.58L571.527 1362.41L571.035 1362.33L570.542 1362.24L585.323 1278.41C585.652 1278.46 585.98 1278.52 586.309 1278.58Z" fill="%2390A4AE"/><path d="M1037.44 1280.26L1036.57 1280.76L994.012 1207.04C994.3 1206.87 994.589 1206.7 994.877 1206.54L1037.44 1280.26Z" fill="%2390A4AE"/><path d="M388.376 1206.54L345.813 1280.26L344.947 1279.76L387.511 1206.04C387.799 1206.2 388.087 1206.37 388.376 1206.54Z" fill="%2390A4AE"/><path d="M1221.44 1125.86L1220.8 1126.63L1155.52 1071.85C1155.74 1071.6 1155.95 1071.34 1156.17 1071.09L1221.44 1125.86Z" fill="%2390A4AE"/><path d="M227.087 1071.09L161.811 1125.86L161.167 1125.1L226.442 1070.32C226.656 1070.58 226.872 1070.83 227.087 1071.09Z" fill="%2390A4AE"/><path d="M1341.54 917.844L1341.2 918.783L1261.27 889.69C1261.38 889.378 1261.5 889.064 1261.61 888.751L1341.54 917.844Z" fill="%2390A4AE"/><path d="M121.642 888.751L41.7109 917.844L41.54 917.374L41.3682 916.903L121.3 887.81C121.414 888.124 121.527 888.437 121.642 888.751Z" fill="%2390A4AE"/><path d="M1383.25 681.293V682.293H1298.03C1298.03 681.96 1298.03 681.626 1298.03 681.293H1383.25Z" fill="%2390A4AE"/><path d="M85.2236 681.293C85.2236 681.626 85.226 681.96 85.2266 682.293H0V681.293H85.2236Z" fill="%2390A4AE"/><path d="M121.641 473.835C121.527 474.148 121.413 474.462 121.3 474.775L41.3682 445.683L41.71 444.743L121.641 473.835Z" fill="%2390A4AE"/><path d="M1341.54 444.743L1261.61 473.836C1261.5 473.522 1261.39 473.208 1261.27 472.895L1341.2 443.803L1341.54 444.743Z" fill="%2390A4AE"/><path d="M227.086 291.497C226.871 291.752 226.656 292.007 226.442 292.263L161.167 237.49L161.488 237.107L161.81 236.724L227.086 291.497Z" fill="%2390A4AE"/><path d="M1221.12 236.341L1221.44 236.724L1156.17 291.497C1155.95 291.242 1155.74 290.986 1155.52 290.731L1220.8 235.958L1221.12 236.341Z" fill="%2390A4AE"/><path d="M388.376 156.047C388.087 156.214 387.798 156.381 387.51 156.549L344.947 82.8271L345.813 82.3271L388.376 156.047Z" fill="%2390A4AE"/><path d="M1037.44 82.3271L994.877 156.047C994.589 155.88 994.3 155.712 994.012 155.546L1036.57 81.8271L1037.44 82.3271Z" fill="%2390A4AE"/><path d="M586.309 84.0059C585.98 84.0634 585.652 84.1207 585.323 84.1787L570.542 0.347656L571.527 0.173828L586.309 84.0059Z" fill="%2390A4AE"/><path d="M811.727 0.173828L796.944 84.0059C796.616 83.9484 796.288 83.8899 795.96 83.833L810.742 0L811.727 0.173828Z" fill="%2390A4AE"/></svg>');
  background-size: cover;
}

.decor {
  animation: rotate 130s linear infinite;
}

.decor.rev {
  animation-direction: reverse;
  animation-duration: 200s;
}

@keyframes rotate {
  from {
    transform: rotate(0deg)
  }

  to {
    transform: rotate(360deg)
  }
}
</style>