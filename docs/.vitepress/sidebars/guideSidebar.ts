export const guideSidebar = [
    {
        text: 'Introduction',
        icon: 'i-solar:star-ring-outline',
        items: [
            { text: 'Getting Started', link: '/guide/getting-started' },
            { text: 'Installation', link: '/guide/installation' },
            { text: 'Core Concepts', link: '/guide/core-concepts' },
        ]
    },
    {
        text: 'Migration Guides',
        icon: 'i-solar:signpost-2-outline',
        collapsed: true,
        items: [
            { text: 'Overview', link: '/guide/migration' },
            { text: 'From Pandas', link: '/guide/migration-pandas' },
            { text: 'From Polars', link: '/guide/migration-polars' },
            { text: 'From Danfo.js', link: '/guide/migration-danfo' },
            { text: 'From Arquero', link: '/guide/migration-arquero' },
        ]
    },
    {
        text: 'Data Types',
        collapsed: true,
        icon: 'i-solar:database-outline',
        items: [
            { text: 'Overview', link: '/guide/data-types' },
            { text: 'Numeric Types', link: '/guide/numeric-types' },
            { text: 'Strings & Dates', link: '/guide/strings-dates' },
        ]
    },
    {
        text: 'Expressions',
        icon: 'i-solar:special-effects-linear',
        collapsed: true,
        items: [
            { text: 'Building Expressions', link: '/guide/expressions' },
            { text: 'Column References', link: '/guide/column-refs' },
            { text: 'Aggregations', link: '/guide/aggregations' },
            { text: 'Joins & Materialization', link: '/guide/mbf-format' }
        ]
    },
    {
        text: 'I/O',
        icon: 'i-solar:square-transfer-horizontal-outline',
        collapsed: true,
        items: [
            { text: 'Reading CSV', link: '/guide/reading-csv' },
            { text: 'Reading Parquet', link: '/guide/reading-parquet' },
            { text: 'Creating DataFrames', link: '/guide/creating-dfs' },
        ]
    }
]
