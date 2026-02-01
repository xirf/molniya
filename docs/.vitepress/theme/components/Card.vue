<script setup lang="ts">
interface Props {
    href: string;
    title: string;
    description: string;
}

const props = defineProps<Props>();
</script>

<template>
    <component :is="props.href ? 'a' : 'div'" role="article" :href="props.href"
        class="clicky flex flex-col gap-1 !text-gray-600 text-sm px-4 py-3 rounded-2xl border border-gray-200 dark:bg-slate-800 dark:border-slate-800 dark:!text-gray-300 interact:shadow-xl !transition-all shadow-gray-700/5 duration-300 !font-normal !no-underline">
        <h3 class="!text-black dark:!text-white !font-medium text-lg !my-0">{{ props.title }}</h3>
        <p class="!m-0 !leading-normal">{{ props.description }}</p>
    </component>
</template>