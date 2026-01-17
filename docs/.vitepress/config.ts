import UnoCSS from 'unocss/vite';
import { defineConfig } from 'vitepress';
import llmstxt from 'vitepress-plugin-llms';

export default defineConfig({
  title: 'Molniya',
  description: 'Ergonomic data analysis for TypeScript',
  head: [['link', { rel: 'icon', href: '/logo.png' }]],
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
  sitemap: { hostname: 'https://molniya.andka.id' },
  themeConfig: {
    logo: '/logo.png',

    nav: [
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'API', link: '/api/dataframe' },
      { text: 'GitHub', link: 'https://github.com/xirf/molniya' },
    ],

    sidebar: {
      '/guide/': [
        {
          text: 'Introduction',
          items: [
            { text: 'Getting Started', link: '/guide/getting-started' },
            { text: 'Core Concepts', link: '/guide/concepts' },
          ],
        },
        {
          text: 'Working with Data',
          items: [
            { text: 'Loading Data', link: '/guide/loading-data' },
            { text: 'Filtering & Sorting', link: '/guide/filtering' },
            { text: 'Grouping & Aggregation', link: '/guide/grouping' },
          ],
        },
        {
          text: 'Usage Guides',
          items: [
            { text: 'Common Recipes', link: '/guide/recipes' },
            { text: 'Performance Guide', link: '/guide/performance' },
            { text: 'Migration Guide', link: '/guide/migration' },
            { text: 'Best Practices', link: '/guide/best-practices' },
            { text: 'Troubleshooting', link: '/guide/troubleshooting' },
          ],
        },
      ],
      '/api/': [
        {
          text: 'API Reference',
          items: [
            { text: 'DataFrame', link: '/api/dataframe' },
            { text: 'Series', link: '/api/series' },
            { text: 'I/O', link: '/api/io' },
          ],
        },
      ],
    },

    socialLinks: [{ icon: 'github', link: 'https://github.com/xirf/molniya' }],

    footer: {
      message: 'Released under the MIT License.',
    },
  },
});
