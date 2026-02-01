<script setup>
const features = [
  {
    icon: '⚡',
    title: 'Columnar Memory',
    desc: 'Uses TypedArrays for efficient storage and SIMD-friendly access. Data is stored column-by-column for improved cache locality.'
  },
  {
    icon: '🌊',
    title: 'Stream Processing',
    desc: 'Processes data in chunks to handle datasets larger than memory. True streaming for CSV with bounded memory usage.'
  },
  {
    icon: '🎯',
    title: 'Lazy Evaluation',
    desc: 'Builds logical plans and executes them in an optimized pipeline. Operations are chained and executed efficiently.'
  },
  {
    icon: '🔤',
    title: 'Dictionary Encoding',
    desc: 'Efficient string handling with automatic dictionary encoding. Reduces memory footprint for repeated string values.'
  },
  {
    icon: '📊',
    title: 'Fluent API',
    desc: 'Expressive chainable API for data manipulation. Write readable, maintainable data transformation code.'
  },
  {
    icon: '🔒',
    title: 'Type Safety',
    desc: 'Full TypeScript support with schema validation. Catch errors at compile time, not runtime.'
  }
]
</script>

<template>
  <div class="features-grid">
    <div v-for="feature in features" :key="feature.title" class="feature-card">
      <div class="icon">{{ feature.icon }}</div>
      <h3 class="title">{{ feature.title }}</h3>
      <p class="desc">{{ feature.desc }}</p>
    </div>
  </div>
</template>

<style scoped>
.features-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 24px;
  padding: 40px 24px;
  max-width: 1200px;
  margin: 0 auto;
}

.feature-card {
  background: var(--vp-c-bg-soft);
  border: 1px solid var(--vp-c-border);
  border-radius: 12px;
  padding: 24px;
  transition: all 0.3s ease;
}

.feature-card:hover {
  transform: translateY(-4px);
  border-color: var(--vp-c-brand-1);
  box-shadow: 0 8px 20px rgba(0, 0, 0, 0.1);
}

.icon {
  font-size: 32px;
  margin-bottom: 16px;
}

.title {
  font-size: 20px;
  font-weight: 600;
  margin-bottom: 12px;
  color: var(--vp-c-text-1);
}

.desc {
  font-size: 16px;
  color: var(--vp-c-text-2);
  line-height: 1.6;
}
</style>
