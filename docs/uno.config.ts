import { defineConfig, presetIcons, presetWind4, transformerDirectives } from 'unocss';

export default defineConfig({
    presets: [
        presetWind4(),
        presetIcons({
            collections: {
                solar: () => import('@iconify-json/solar/icons.json').then(i => i.default as any)
            }
        })
    ],
    transformers: [transformerDirectives()],
    content: {
        pipeline: {
            include: [
                '**/*.{md,mdx}',
                '**/*.vue',
                '.vitepress/**/*.{ts,tsx,vue}',
            ]
        }
    },
    safelist: [
        'i-solar:star-ring-outline',
        'i-solar:database-outline',
        'i-solar:special-effects-linear',
        'i-solar:square-transfer-horizontal-outline',
        'i-solar:branching-paths-up-outline',
        'i-solar:window-frame-linear',
        'i-solar:square-transfer-vertical-outline',
        'i-solar:pie-chart-3-outline',
        'i-solar:programming-linear',
        'i-solar:code-file-linear',
        'i-solar:chef-hat-heart-linear',
        'i-solar:signpost-2-outline'
    ]
});