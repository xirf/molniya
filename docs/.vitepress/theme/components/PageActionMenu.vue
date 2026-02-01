<script setup lang="ts">
import { ref, computed } from 'vue'
import { useRoute, useData } from 'vitepress'
import { onClickOutside } from '@vueuse/core'

const route = useRoute()
const { page } = useData()
const isOpen = ref(false)
const menuRef = ref(null)
const formatCopied = ref(false)
const sourceCopied = ref(false)

onClickOutside(menuRef, () => isOpen.value = false)

// --- Logic ---
const pagePath = computed(() => route.path.replace(/(index)?\.html$/, ''))
const rawMdPath = computed(() => `${pagePath.value}.md`)

const prompt = computed(() =>
  encodeURIComponent(
    `Read https://molniya.andka.id${rawMdPath.value} so I can ask questions about it.`
  )
)

const chatGptUrl = computed(() => `https://chatgpt.com/?hints=search&q=${prompt.value}`)
const claudeUrl = computed(() => `https://claude.ai/new?q=${prompt.value}`)
const t3Url = computed(() => `https://t3.chat/new?q=${prompt.value}`)
const perplexityUrl = computed(() => `https://www.perplexity.ai/search/new?q=${prompt.value}`)
const grokUrl = computed(() => `https://x.com/i/grok?text=${prompt.value}`)
const sciraUrl = computed(() => `https://scira.ai/?q=${prompt.value}`)
const geminiUrl = computed(() => `https://www.google.com/search?udm=50&aep=11&q=${prompt.value}`)
const githubEditUrl = computed(() => `https://github.com/xirf/Molniya/edit/main/docs/${rawMdPath.value.replace(/^\//,'')}`)

// Actions
function copyMarkdownLink() {
  const url = window.location.href
  const title = page.value.title
  navigator.clipboard.writeText(`[${title}](${url})`)
  formatCopied.value = true
  setTimeout(() => formatCopied.value = false, 2000)
  isOpen.value = false
}

function copySource() {
  const url = `${window.location.origin}${rawMdPath.value}`
  fetch(url)
    .then(res => res.text())
    .then(text => {
      navigator.clipboard.writeText(text)
      sourceCopied.value = true
      setTimeout(() => sourceCopied.value = false, 2000)
    })
    .catch(console.error)
  isOpen.value = false
}
</script>

<template>
  <div class="relative" ref="menuRef">
    <button 
      class="flex items-center gap-2 px-3 py-1.5 text-xs font-medium rounded-md border border-[var(--vp-c-divider)] hover:border-[var(--vp-c-text-2)] hover:text-[var(--vp-c-text-1)] transition-colors bg-[var(--vp-c-bg-soft)] text-[var(--vp-c-text-2)]" 
      @click="isOpen = !isOpen"
    >
      <span v-if="sourceCopied || formatCopied" class="i-solar:check-circle-bold text-green-500"></span>
      <span v-else class="i-solar:copy-linear"></span>
      <span>Copy Page</span>
      <span class="i-solar:alt-arrow-down-linear text-[10px] opacity-70"></span>
    </button>

    <div 
      v-if="isOpen"
      class="absolute right-0 top-full mt-1 max-h-50vh md:max-h-80vh overflow-y-auto w-56 p-1 z-50 origin-top-right rounded-lg border border-[var(--vp-c-divider)] bg-[var(--vp-c-bg-soft)]/25 shadow-xl ring-1 ring-black/5 focus:outline-none backdrop-blur-lg"
    >
      <!-- Section 1: Standard Copies -->
      <div class="p-1 space-y-0.5">
        <a :href="githubEditUrl" target="_blank" class="menu-item">
            <span class="i-solar:pen-linear size-4 text-[var(--vp-c-text-2)]"></span>
            <span class="flex-1">Edit on GitHub</span>
            <span class="i-solar:arrow-right-up-linear size-3 opacity-50"></span>
        </a>

        <button @click="copyMarkdownLink" class="menu-item">
          <span class="i-solar:link-linear size-4 text-[var(--vp-c-text-2)]"></span>
          <span class="flex-1">Copy Markdown link</span>
        </button>
        
        <button @click="copySource" class="menu-item">
            <span class="i-solar:copy-linear size-4 text-[var(--vp-c-text-2)]"></span>
            <span class="flex-1">Copy Markdown content</span>
        </button>

         <a :href="rawMdPath" target="_blank" class="menu-item">
          <span class="i-solar:document-text-linear size-4 text-[var(--vp-c-text-2)]"></span>
          <span class="flex-1">View as Markdown</span>
          <span class="i-solar:arrow-right-up-linear size-3 opacity-50"></span>
        </a>
      </div>

      <div class="h-px bg-[var(--vp-c-divider)] my-1 mx-2"></div>

      <!-- Section 2: External/AI -->
      <div class="p-1 space-y-0.5">
        <a :href="sciraUrl" target="_blank" class="menu-item">
            <!-- Scira Icon -->
            <svg class="size-4 text-[var(--vp-c-text-2)]" viewBox="0 0 910 934" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M647.664 197.775C569.13 189.049 525.5 145.419 516.774 66.8849C508.048 145.419 464.418 189.049 385.884 197.775C464.418 206.501 508.048 250.131 516.774 328.665C525.5 250.131 569.13 206.501 647.664 197.775Z" fill="currentColor" stroke="currentColor" stroke-width="8" stroke-linejoin="round"></path><path d="M516.774 304.217C510.299 275.491 498.208 252.087 480.335 234.214C462.462 216.341 439.058 204.251 410.333 197.775C439.059 191.3 462.462 179.209 480.335 161.336C498.208 143.463 510.299 120.06 516.774 91.334C523.25 120.059 535.34 143.463 553.213 161.336C571.086 179.209 594.49 191.3 623.216 197.775C594.49 204.251 571.086 216.341 553.213 234.214C535.34 252.087 523.25 275.491 516.774 304.217Z" fill="currentColor" stroke="currentColor" stroke-width="8" stroke-linejoin="round"></path><path d="M857.5 508.116C763.259 497.644 710.903 445.288 700.432 351.047C689.961 445.288 637.605 497.644 543.364 508.116C637.605 518.587 689.961 570.943 700.432 665.184C710.903 570.943 763.259 518.587 857.5 508.116Z" stroke="currentColor" stroke-width="20" stroke-linejoin="round"></path><path d="M700.432 615.957C691.848 589.05 678.575 566.357 660.383 548.165C642.191 529.973 619.499 516.7 592.593 508.116C619.499 499.533 642.191 486.258 660.383 468.066C678.575 449.874 691.848 427.181 700.432 400.274C709.015 427.181 722.289 449.874 740.481 468.066C758.673 486.258 781.365 499.533 808.271 508.116C781.365 516.7 758.673 529.973 740.481 548.165C722.289 566.357 709.015 589.05 700.432 615.957Z" stroke="currentColor" stroke-width="20" stroke-linejoin="round"></path><path d="M889.949 121.237C831.049 114.692 798.326 81.9698 791.782 23.0692C785.237 81.9698 752.515 114.692 693.614 121.237C752.515 127.781 785.237 160.504 791.782 219.404C798.326 160.504 831.049 127.781 889.949 121.237Z" fill="currentColor" stroke="currentColor" stroke-width="8" stroke-linejoin="round"></path><path d="M791.782 196.795C786.697 176.937 777.869 160.567 765.16 147.858C752.452 135.15 736.082 126.322 716.226 121.237C736.082 116.152 752.452 107.324 765.16 94.6152C777.869 81.9065 786.697 65.5368 791.782 45.6797C796.867 65.5367 805.695 81.9066 818.403 94.6152C831.112 107.324 847.481 116.152 867.338 121.237C847.481 126.322 831.112 135.15 818.403 147.858C805.694 160.567 796.867 176.937 791.782 196.795Z" fill="currentColor" stroke="currentColor" stroke-width="8" stroke-linejoin="round"></path><path d="M760.632 764.337C720.719 814.616 669.835 855.1 611.872 882.692C553.91 910.285 490.404 924.255 426.213 923.533C362.022 922.812 298.846 907.419 241.518 878.531C184.19 849.643 134.228 808.026 95.4548 756.863C56.6815 705.7 30.1238 646.346 17.8129 583.343C5.50207 520.339 7.76433 455.354 24.4266 393.359C41.089 331.364 71.7099 274.001 113.947 225.658C156.184 177.315 208.919 139.273 268.117 114.442" stroke="currentColor" stroke-width="30" stroke-linecap="round" stroke-linejoin="round"></path></svg>
          <span class="flex-1">Open in Scira AI</span>
          <span class="i-solar:arrow-right-up-linear size-3 opacity-50"></span>
        </a>

        <a :href="chatGptUrl" target="_blank" class="menu-item">
            <!-- ChatGPT Icon -->
             <svg class="size-4 text-[var(--vp-c-text-2)]" preserveAspectRatio="xMidYMid" viewBox="0 0 256 260" fill="currentColor"><path d="M239.184 106.203a64.716 64.716 0 0 0-5.576-53.103C219.452 28.459 191 15.784 163.213 21.74A65.586 65.586 0 0 0 52.096 45.22a64.716 64.716 0 0 0-43.23 31.36c-14.31 24.602-11.061 55.634 8.033 76.74a64.665 64.665 0 0 0 5.525 53.102c14.174 24.65 42.644 37.324 70.446 31.36a64.72 64.72 0 0 0 48.754 21.744c28.481.025 53.714-18.361 62.414-45.481a64.767 64.767 0 0 0 43.229-31.36c14.137-24.558 10.875-55.423-8.083-76.483Zm-97.56 136.338a48.397 48.397 0 0 1-31.105-11.255l1.535-.87 51.67-29.825a8.595 8.595 0 0 0 4.247-7.367v-72.85l21.845 12.636c.218.111.37.32.409.563v60.367c-.056 26.818-21.783 48.545-48.601 48.601Zm-104.466-44.61a48.345 48.345 0 0 1-5.781-32.589l1.534.921 51.722 29.826a8.339 8.339 0 0 0 8.441 0l63.181-36.425v25.221a.87.87 0 0 1-.358.665l-52.335 30.184c-23.257 13.398-52.97 5.431-66.404-17.803ZM23.549 85.38a48.499 48.499 0 0 1 25.58-21.333v61.39a8.288 8.288 0 0 0 4.195 7.316l62.874 36.272-21.845 12.636a.819.819 0 0 1-.767 0L41.353 151.53c-23.211-13.454-31.171-43.144-17.804-66.405v.256Zm179.466 41.695-63.08-36.63L161.73 77.86a.819.819 0 0 1 .768 0l52.233 30.184a48.6 48.6 0 0 1-7.316 87.635v-61.391a8.544 8.544 0 0 0-4.4-7.213Zm21.742-32.69-1.535-.922-51.619-30.081a8.39 8.39 0 0 0-8.492 0L99.98 99.808V74.587a.716.716 0 0 1 .307-.665l52.233-30.133a48.652 48.652 0 0 1 72.236 50.391v.205ZM88.061 139.097l-21.845-12.585a.87.87 0 0 1-.41-.614V65.685a48.652 48.652 0 0 1 79.757-37.346l-1.535.87-51.67 29.825a8.595 8.595 0 0 0-4.246 7.367l-.051 72.697Zm11.868-25.58 28.138-16.217 28.188 16.218v32.434l-28.086 16.218-28.188-16.218-.052-32.434Z" /></svg>
          <span class="flex-1">Open in ChatGPT</span>
          <span class="i-solar:arrow-right-up-linear size-3 opacity-50"></span>
        </a>

        <a :href="claudeUrl" target="_blank" class="menu-item">
            <!-- Claude Icon -->
            <svg class="size-4 text-[var(--vp-c-text-2)]" fill-rule="evenodd" viewBox="0 0 24 24" fill="currentColor"><path d="M13.827 3.52h3.603L24 20h-3.603l-6.57-16.48zm-7.258 0h3.767L16.906 20h-3.674l-1.343-3.461H5.017l-1.344 3.46H0L6.57 3.522zm4.132 9.959L8.453 7.687 6.205 13.48H10.7z" /></svg>
          <span class="flex-1">Open in Claude</span>
          <span class="i-solar:arrow-right-up-linear size-3 opacity-50"></span>
        </a>

        <a :href="t3Url" target="_blank" class="menu-item">
            <!-- T3 Icon -->
            <svg class="size-4 text-[var(--vp-c-text-2)]" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2.992 16.342a2 2 0 0 1 .094 1.167l-1.065 3.29a1 1 0 0 0 1.236 1.168l3.413-.998a2 2 0 0 1 1.099.092 10 10 0 1 0-4.777-4.719"></path></svg>
          <span class="flex-1">Open in T3 Chat</span>
          <span class="i-solar:arrow-right-up-linear size-3 opacity-50"></span>
        </a>

        <a :href="perplexityUrl" target="_blank" class="menu-item">
            <!-- Perplexity Icon -->
            <svg class="size-4 text-[var(--vp-c-text-2)]" xmlns="http://www.w3.org/2000/svg" fill="currentColor" fill-rule="evenodd" viewBox="0 0 24 24"><path d="M19.785 0v7.272H22.5V17.62h-2.935V24l-7.037-6.194v6.145h-1.091v-6.152L4.392 24v-6.465H1.5V7.188h2.884V0l7.053 6.494V.19h1.09v6.49L19.786 0zm-7.257 9.044v7.319l5.946 5.234V14.44l-5.946-5.397zm-1.099-.08l-5.946 5.398v7.235l5.946-5.234V8.965zm8.136 7.58h1.844V8.349H13.46l6.105 5.54v2.655zm-8.982-8.28H2.59v8.195h1.8v-2.576l6.192-5.62zM5.475 2.476v4.71h5.115l-5.115-4.71zm13.219 0l-5.115 4.71h5.115v-4.71z"/></svg>
          <span class="flex-1">Open in Perplexity</span>
          <span class="i-solar:arrow-right-up-linear size-3 opacity-50"></span>
        </a>

        <a :href="grokUrl" target="_blank" class="menu-item">
             <!-- Grok Icon -->
            <svg class="size-4 text-[var(--vp-c-text-2)]" xmlns="http://www.w3.org/2000/svg" fill="currentColor" fill-rule="evenodd" viewBox="0 0 24 24"><path d="M9.27 15.29l7.978-5.897c.391-.29.95-.177 1.137.272.98 2.369.542 5.215-1.41 7.169-1.951 1.954-4.667 2.382-7.149 1.406l-2.711 1.257c3.889 2.661 8.611 2.003 11.562-.953 2.341-2.344 3.066-5.539 2.388-8.42l.006.007c-.983-4.232.242-5.924 2.75-9.383.06-.082.12-.164.179-.248l-3.301 3.305v-.01L9.267 15.292M7.623 16.723c-2.792-2.67-2.31-6.801.071-9.184 1.761-1.763 4.647-2.483 7.166-1.425l2.705-1.25a7.808 7.808 0 00-1.829-1A8.975 8.975 0 005.984 5.83c-2.533 2.536-3.33 6.436-1.962 9.764 1.022 2.487-.653 4.246-2.34 6.022-.599.63-1.199 1.259-1.682 1.925l7.62-6.815"/></svg>
            <span class="flex-1">Open in Grok</span>
            <span class="i-solar:arrow-right-up-linear size-3 opacity-50"></span>
        </a>

        <a :href="geminiUrl" target="_blank" class="menu-item">
             <!-- Gemini Icon -->
            <svg class="size-4 text-[var(--vp-c-text-2)]" xmlns="http://www.w3.org/2000/svg" fill="currentColor" fill-rule="evenodd" viewBox="0 0 24 24"><path d="M20.616 10.835a14.147 14.147 0 01-4.45-3.001 14.111 14.111 0 01-3.678-6.452.503.503 0 00-.975 0 14.134 14.134 0 01-3.679 6.452 14.155 14.155 0 01-4.45 3.001c-.65.28-1.318.505-2.002.678a.502.502 0 000 .975c.684.172 1.35.397 2.002.677a14.147 14.147 0 014.45 3.001 14.112 14.112 0 013.679 6.453.502.502 0 00.975 0c.172-.685.397-1.351.677-2.003a14.145 14.145 0 013.001-4.45 14.113 14.113 0 016.453-3.678.503.503 0 000-.975 13.245 13.245 0 01-2.003-.678z"/></svg>
            <span class="flex-1">Open in Gemini</span>
            <span class="i-solar:arrow-right-up-linear size-3 opacity-50"></span>
        </a>
      </div>
    </div>
  </div>
</template>

<style scoped>
.menu-item {
  display: flex;
  width: 100%;
  align-items: center;
  gap: 0.5rem;
  border-radius: 0.25rem;
  padding: 0.375rem 0.5rem;
  text-align: left;
  font-size: 0.75rem;
  color: var(--vp-c-text-1);
  transition: background-color 0.2s, color 0.2s;
}

.menu-item:hover {
  background-color: var(--vp-c-bg-soft);
  color: var(--vp-c-brand-1);
}
</style>
