import { transformerTwoslash } from '@shikijs/vitepress-twoslash';
import { createFileSystemTypesCache } from '@shikijs/vitepress-twoslash/cache-fs';
import UnoCSS from 'unocss/vite';
import { defineConfig } from 'vitepress';
import llmstxt from 'vitepress-plugin-llms';

export default defineConfig({
  title: 'Molniya',
  description: 'Ergonomic data analysis for TypeScript',
  lastUpdated: true,
  ignoreDeadLinks: false,
  lang: 'en-US',
  head: [
    ['link', { rel: 'icon', href: '/logo.png' }],
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
      process.env.NODE_ENV === 'production'
        ? llmstxt({
            description: 'Ergonomic data analysis for TypeScript',
            details: 'Ergonomic data analysis for TypeScript',
            ignoreFiles: ['index.md', 'table-of-content.md', 'blog/*', 'public/*'],
            domain: 'https://molniya.andka.id',
          })
        : undefined,
    ],
  },
  markdown: {
    codeTransformers: [
      transformerTwoslash({
        typesCache: createFileSystemTypesCache({
          dir: './docs/.vitepress/cache/twoslash',
        }),
        twoslashOptions: {
          compilerOptions: {
            paths: {
              molniya: ['./src/index.ts'],
            },
          },
        },
      }),
    ],
  },
  sitemap: { hostname: 'https://molniya.andka.id' },
  themeConfig: {
    logo: '/logo.png',

    nav: [
      { text: 'Guide', link: '/guide/introduction' },
      { text: 'API', link: '/api/overview' },
      { text: 'Cookbook', link: '/cookbook/' },
      { text: 'GitHub', link: 'https://github.com/xirf/molniya' },
    ],

    sidebar: {
      '/guide/': [
        {
          text: 'Getting Started',
          items: [
            { text: 'Introduction', link: '/guide/introduction' },
            { text: 'Getting Started', link: '/guide/getting-started' },
            { text: 'What is Molniya?', link: '/guide/what-is-molniya' },
          ],
        },
        {
          text: 'Core Concepts',
          items: [
            { text: 'Data Types', link: '/guide/data-types' },
            { text: 'Working with CSV', link: '/guide/io-csv' },
            { text: 'Lazy Evaluation', link: '/guide/lazy-evaluation' },
          ],
        },
        {
          text: 'Migration',
          items: [
            { text: 'Overview', link: '/guide/migration/' },
            { text: 'From Pandas', link: '/guide/migration/from-pandas' },
            { text: 'From Polars', link: '/guide/migration/from-polars' },
            { text: 'From Arquero', link: '/guide/migration/from-arquero' },
            { text: 'From Danfo.js', link: '/guide/migration/from-danfo' },
          ],
        },
      ],
      '/cookbook/': [
        {
          text: '🍳 Cookbook',
          items: [
            { text: 'Overview', link: '/cookbook/' },
            { text: 'DataFrame', link: '/cookbook/dataframe' },
            { text: 'LazyFrame', link: '/cookbook/lazyframe' },
            { text: 'Series', link: '/cookbook/series' },
            { text: 'CSV I/O', link: '/cookbook/csv-io' },
            { text: 'Data Cleaning', link: '/cookbook/cleaning' },
          ],
        },
      ],
      '/api/': [
        {
          text: 'API Reference',
          items: [{ text: 'Overview', link: '/api/overview' }],
        },
        {
          text: 'Core Classes',
          items: [
            { text: 'DataFrame', link: '/api/dataframe' },
            { text: 'LazyFrame', link: '/api/lazyframe' },
            { text: 'Series', link: '/api/series' },
          ],
        },
        {
          text: 'I/O Functions',
          items: [
            { text: 'CSV Reading', link: '/api/csv-reading' },
            { text: 'CSV Writing', link: '/api/csv-writing' },
          ],
        },
        {
          text: 'Types',
          items: [
            { text: 'DType', link: '/api/dtype' },
            { text: 'Result', link: '/api/result' },
            { text: 'Schema', link: '/api/schema' },
          ],
        },
      ],
    },

    outline: {
      level: [2, 3],
      label: 'On this page',
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/xirf/molniya' },
      { icon: 'npm', link: 'https://www.npmjs.com/package/molniya' },
    ],

    editLink: {
      text: 'Edit this page on GitHub',
      pattern: 'https://github.com/xirf/molniya/edit/main/docs/:path',
    },

    search: {
      provider: 'local',
    },

    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2024-present Molniya Contributors',
    },
  },
});
