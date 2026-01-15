import { defineConfig } from 'vitepress';

export default defineConfig({
  title: 'Mornye',
  description: 'High-performance data manipulation for Bun.js',

  head: [['meta', { name: 'theme-color', content: '#5c6bc0' }]],

  themeConfig: {
    logo: '/logo.svg',

    nav: [
      { text: 'Guide', link: '/getting-started' },
      { text: 'API', link: '/api/series' },
      { text: 'GitHub', link: 'https://github.com/yourname/mornye' },
    ],

    sidebar: {
      '/': [
        {
          text: 'Introduction',
          items: [
            { text: 'Getting Started', link: '/getting-started' },
            { text: 'Why Mornye?', link: '/why-mornye' },
          ],
        },
        {
          text: 'Core Concepts',
          items: [
            { text: 'Data Cleaning', link: '/concepts/cleaning' },
            { text: 'String Manipulation', link: '/concepts/strings' },
          ],
        },
        {
          text: 'API Reference',
          items: [
            { text: 'Series', link: '/api/series' },
            { text: 'DataFrame', link: '/api/dataframe' },
            { text: 'LazyFrame', link: '/api/lazyframe' },
            { text: 'CSV I/O', link: '/api/csv' },
          ],
        },
      ],
    },

    socialLinks: [{ icon: 'github', link: 'https://github.com/yourname/mornye' }],

    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2024',
    },
  },
});
