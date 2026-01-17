import { defineConfig } from 'vitepress';

export default defineConfig({
  title: 'Mornye',
  description: 'Ergonomic data analysis for TypeScript',
  head: [['link', { rel: 'icon', href: '/logo.png' }]],

  themeConfig: {
    logo: '/logo.png',

    nav: [
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'API', link: '/api/dataframe' },
      { text: 'GitHub', link: 'https://github.com/xirf/mornye' },
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

    socialLinks: [{ icon: 'github', link: 'https://github.com/xirf/mornye' }],

    footer: {
      message: 'Released under the MIT License.',
    },
  },
});
