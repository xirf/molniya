<script setup lang="ts">
import { ref, shallowRef, onMounted, onUpdated, computed, nextTick } from 'vue'

const headers = shallowRef<{ element: HTMLElement; title: string; link: string; slug: string; level: number }[]>([])
const container = ref<HTMLElement | null>(null)
const markerTop = ref(0)
const markerHeight = ref(20)
const markerOpacity = ref(0)
const activeSlugs = shallowRef<Set<string>>(new Set())
const containerHeight = ref(100)
const itemRefs = ref<Record<string, HTMLElement>>({})

const IGNORE_CLASSES = ['VPBadge', 'header-anchor', 'ignore-header']

function serializeHeader(h: Element): string {
    let text = ''
    for (const node of h.childNodes) {
        if (node.nodeType === 1) {
            const el = node as Element
            if (!IGNORE_CLASSES.some(c => el.classList.contains(c))) {
                text += el.textContent
            }
        } else if (node.nodeType === 3) {
            text += node.textContent
        }
    }
    return text.trim()
}

function resolveHeaders() {
    const elements = document.querySelectorAll('.VPDoc h2, .VPDoc h3, .VPDoc h4, .VPDoc h5, .VPDoc h6')
    return Array.from(elements)
        .filter(el => el.id && el.hasChildNodes())
        .map(el => ({
            element: el as HTMLElement,
            title: serializeHeader(el),
            link: '#' + el.id,
            slug: el.id,
            level: Number(el.tagName[1])
        }))
}

function getAbsoluteTop(element: HTMLElement): number {
    let offsetTop = 0
    while (element && element !== document.body) {
        offsetTop += element.offsetTop
        element = element.offsetParent as HTMLElement
    }
    return element ? offsetTop : NaN
}

function setActiveLink() {
    if (!container.value || !headers.value.length) {
        markerOpacity.value = 0
        return
    }

    const scrollY = window.scrollY
    const innerHeight = window.innerHeight
    const scrollHeight = document.documentElement.scrollHeight
    const isBottom = Math.abs(scrollY + innerHeight - scrollHeight) < 20

    const headerMap = headers.value
        .map(h => ({ slug: h.slug, top: getAbsoluteTop(h.element) }))
        .filter(h => !isNaN(h.top))
        .sort((a, b) => a.top - b.top)

    if (!headerMap.length) {
        markerOpacity.value = 0
        return
    }

    const topOffset = 100
    const viewportTop = scrollY + topOffset
    const viewportBottom = scrollY + innerHeight * (scrollY < 50 ? 1.0 : 0.8)

    const visible = headerMap.filter(h => h.top >= viewportTop && h.top < viewportBottom)
    const activeAbove = headerMap.findLast(h => h.top < viewportTop)

    const activeList = [...(activeAbove ? [activeAbove] : []), ...visible]

    if (isBottom) {
        const last = headerMap[headerMap.length - 1]
        if (!activeList.some(x => x.slug === last.slug)) activeList.push(last)
    }

    if (!activeList.length) {
        markerOpacity.value = 0
        activeSlugs.value = new Set()
        return
    }

    activeSlugs.value = new Set(activeList.map(h => h.slug))

    const firstEl = itemRefs.value[activeList[0].slug]
    const lastEl = itemRefs.value[activeList[activeList.length - 1].slug]

    if (firstEl && lastEl) {
        const containerRect = container.value.getBoundingClientRect()
        const firstRect = firstEl.getBoundingClientRect()
        const lastRect = lastEl.getBoundingClientRect()

        markerTop.value = firstRect.top - containerRect.top
        markerHeight.value = Math.max(lastRect.bottom - firstRect.top, 20)
        markerOpacity.value = 1
    } else {
        markerOpacity.value = 0
    }
}

function throttle(fn: Function, delay: number) {
    let last = 0
    return () => {
        const now = Date.now()
        if (now - last > delay) {
            last = now
            fn()
        }
    }
}

const setItemRef = (slug: string) => (el: HTMLElement | null) => {
    if (el) itemRefs.value[slug] = el
}

function handleClick(e: MouseEvent, slug: string) {
    activeSlugs.value = new Set([slug])
    nextTick(setActiveLink)
}

onMounted(() => {
    headers.value = resolveHeaders()

    const observer = new MutationObserver(() => {
        headers.value = resolveHeaders()
        setActiveLink()
    })

    const doc = document.querySelector('.VPDoc')
    if (doc) observer.observe(doc, { childList: true, subtree: true })

    window.addEventListener('scroll', throttle(setActiveLink, 100))
    window.addEventListener('resize', throttle(setActiveLink, 100))

    setTimeout(() => {
        headers.value = resolveHeaders()
        setActiveLink()
    }, 500)

    requestAnimationFrame(setActiveLink)
})

onUpdated(() => {
    if (container.value) containerHeight.value = container.value.offsetHeight
})

const maskSvgUrl = computed(() => {
    if (!headers.value.length) return ''

    const itemHeight = 28.5
    const startY = 0
    let currentX = 1
    let currentY = 0
    let path = `M ${currentX} ${currentY} `

    headers.value.forEach((h, i) => {
        const targetX = h.level > 2 ? 11 : 1
        const itemCenterY = startY + (i * itemHeight) + (itemHeight / 2)

        if (targetX !== currentX) {
            const turnY = itemCenterY - (itemHeight)
            if (turnY > currentY) {
                path += `L ${currentX} ${turnY + 10}`
                currentY = turnY
            }
            path += `L ${targetX} ${itemCenterY - 4} `
            currentX = targetX
            currentY = itemCenterY - 20
        }

        const nextY = startY + ((i + 1) * itemHeight) - 8
        path += `L ${currentX} ${nextY} `
        currentY = nextY + 8
    })

    const totalH = Math.max(containerHeight.value, currentY)
    path += `L ${currentX} ${totalH}`

    const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 14 ${totalH}" preserveAspectRatio="none"><path d="${path}" stroke="black" stroke-width="2" fill="none" vector-effect="non-scaling-stroke"/></svg>`
    return `url("data:image/svg+xml,${encodeURIComponent(svg)}")`
})
</script>

<template>
    <div class="VPOutline" v-show="headers.length > 0">
        <div class="flex items-center gap-2 px-2 mb-2">
            <div class="i-solar:list-linear size-5 block"></div> On this page
        </div>

        <div class="content-wrapper" ref="container">
            <div class="track-column">
                <div class="track-mask" :style="{ maskImage: maskSvgUrl, '-webkit-mask-image': maskSvgUrl }">
                    <div class="track-dim"></div>
                    <div class="track-active" :style="{
                        top: `${markerTop}px`,
                        height: `${markerHeight}px`,
                        opacity: markerOpacity
                    }">
                    </div>
                </div>
            </div>

            <nav class="outline-nav">
                <ul class="root-list">
                    <li v-for="header in headers" :key="header.slug" class="outline-item">
                        <a :href="header.link" :ref="setItemRef(header.slug) as any"
                            :class="{ active: activeSlugs.has(header.slug) }"
                            :style="{ paddingLeft: (header.level - 2) * 12 + 'px' }"
                            @click="handleClick($event, header.slug)">
                            {{ header.title }}
                        </a>
                    </li>
                </ul>
            </nav>
        </div>
    </div>
</template>

<style scoped>
.VPOutline {
    position: sticky;
    font-size: 13px;
    font-weight: 500;
    display: flex;
    flex-direction: column;
}

.outline-header {
    font-weight: 600;
    margin-bottom: 0.5rem;
    padding-left: 1rem;
    color: var(--vp-c-text-1);
    letter-spacing: 0.4px;
}

.content-wrapper {
    position: relative;
    display: flex;
}

.track-column {
    position: absolute;
    top: 0;
    left: 12px;
    bottom: 0;
    width: 14px;
}

.track-mask {
    position: absolute;
    inset: 0;
    mask-repeat: no-repeat;
    mask-position: top left;
    -webkit-mask-repeat: no-repeat;
    -webkit-mask-position: top left;
}

.track-dim {
    position: absolute;
    inset: 0;
    background-color: var(--vp-c-divider);
    opacity: 0.4;
}

.track-active {
    position: absolute;
    left: 0;
    right: 0;
    background-color: var(--vp-c-brand-1);
    transition: top 0.2s ease, height 0.2s ease, opacity 0.2s ease;
    box-shadow: 0 0 6px var(--vp-c-brand-1);
}

.outline-nav {
    padding-left: 24px;
    width: 100%;
}

.outline-item {
    line-height: 28px;
    color: var(--vp-c-text-2);
    transition: color 0.2s;
}

.outline-item a {
    display: block;
    text-decoration: none;
    color: inherit;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    opacity: 0.8;
}

.outline-item a:hover {
    color: var(--vp-c-text-1);
    opacity: 1;
}

.outline-item a.active {
    color: var(--vp-c-brand-1);
    opacity: 1;
}
</style>
