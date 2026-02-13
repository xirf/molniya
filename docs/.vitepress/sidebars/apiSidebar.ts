export const apiSidebar = [
    {
        text: 'API Reference',
        icon: 'i-solar:branching-paths-up-outline',
        items: [
            { text: 'Overview', link: '/api/' },
        ]
    },
    {
        text: 'DataFrame',
        icon: 'i-solar:window-frame-linear',
        items: [
            { text: 'DataFrame Class', link: '/api/dataframe' },
            { text: 'Creation Functions', link: '/api/dataframe-creation' },
            { text: 'Inspection Methods', link: '/api/inspection' },
            { text: 'Execution Methods', link: '/api/execution' },
        ]
    },
    {
        text: 'Transformations',
        icon: 'i-solar:square-transfer-vertical-outline',
        items: [
            { text: 'Filtering', link: '/api/filtering' },
            { text: 'Projection', link: '/api/projection' },
            { text: 'Column Operations', link: '/api/column-ops' },
            { text: 'Sorting & Limiting', link: '/api/sort-limit' },
        ]
    },
    {
        text: 'Aggregation & Joins',
        icon: 'i-solar:pie-chart-3-outline',
        items: [
            { text: 'GroupBy', link: '/api/groupby' },
            { text: 'Aggregation Functions', link: '/api/aggregations' },
            { text: 'Joins', link: '/api/joins' },
        ]
    },
    {
        text: 'Expressions',
        icon: 'i-solar:programming-linear',
        items: [
            { text: 'Expression Builders', link: '/api/expr-builders' },
            { text: 'ColumnRef', link: '/api/column-ref' },
            { text: 'Operators', link: '/api/operators' },
        ]
    },
    {
        text: 'Data Types',
        icon: 'i-solar:code-file-linear',
        items: [
            { text: 'DType', link: '/api/dtype' },
            { text: 'DTypeKind', link: '/api/dtype-kind' },
            { text: 'Schema', link: '/api/schema' },
        ]
    },
    {
        text: 'I/O',
        icon: 'i-solar:square-transfer-horizontal-outline',
        items: [
            { text: 'CSV', link: '/api/csv' },
            { text: 'Parquet', link: '/api/parquet' },
        ]
    },
    {
        text: 'Visualization',
        icon: 'i-solar:chart-2-outline',
        items: [
            { text: 'Plotting', link: '/api/plotting' },
        ]
    }
]