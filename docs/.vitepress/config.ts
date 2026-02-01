import { fileURLToPath, URL } from 'node:url'
import { transformerTwoslash } from '@shikijs/vitepress-twoslash'
import UnoCSS from 'unocss/vite';
import { defineConfig, type DefaultTheme } from 'vitepress';
import llmstxt from 'vitepress-plugin-llms';
import { guideSidebar } from './sidebars/guideSidebar';
import { cookbookSidebar } from './sidebars/cookbookSidebar';
import { apiSidebar } from './sidebars/apiSidebar';
import { exampleSidebar } from './sidebars/exampleSidebar';

export default defineConfig({
  title: 'Molniya',
  description: 'Ergonomic data analysis for TypeScript',
  outDir: './dist',
  lastUpdated: true,
  ignoreDeadLinks: false,
  lang: 'en-US',
  head: [
    ['link', { rel: 'icon', href: '/molniya.svg' }],
    ['link', { rel: 'preconnect', href: 'https://fonts.googleapis.com' }],
    ['link', { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' }],
    [
      'link',
      {
        rel: 'stylesheet',
        href: 'https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=JetBrains+Mono:wght@400;500&display=swap',
      },
    ],
  ],
  vite: {
    plugins: [
      UnoCSS(),
      ...llmstxt({
        domain: 'https://molniya.andka.id',
      })
    ],
    resolve: {
      alias: [
        {
          find: /^.*\/VPDocAsideOutline\.vue$/,
          replacement: fileURLToPath(
            new URL('./theme/components/DocOutline.vue', import.meta.url)
          )
        },
        {
          find: /^.*\/VPSwitchAppearance\.vue$/,
          replacement: fileURLToPath(
            new URL('./theme/components/ThemeSwitcher.vue', import.meta.url)
          )
        },
        {
          find: /^.*\/VPSidebarItem\.vue$/,
          replacement: fileURLToPath(
            new URL('./theme/components/VPSidebarItem.vue', import.meta.url)
          )
        }
      ]
    }
  },
  markdown: {
    theme: {
      light: 'catppuccin-latte',
      dark: 'catppuccin-macchiato',
    },
    codeTransformers: [
      transformerTwoslash(),
    ],
  },
  sitemap: { hostname: 'https://molniya.andka.id' },
  themeConfig: {
    logo: '/molniya.svg',

    nav: [
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'Cookbook', link: '/cookbook/' },
      { text: 'API Reference', link: '/api/' },
      { text: 'Examples', link: '/examples/' },
    ],

    sidebar: {
      '/guide/': guideSidebar,
      '/cookbook/': cookbookSidebar,
      '/api/': apiSidebar,
      '/examples/': exampleSidebar
    } satisfies DefaultTheme.Sidebar,

    outline: {
      level: [2, 3],
      label: 'On this page'
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/xirf/Molniya' }
    ],

    editLink: {
      pattern: 'https://github.com/xirf/Molniya/edit/main/docs/:path',
      text: 'Suggest changes to this page'
    },
  }
})
