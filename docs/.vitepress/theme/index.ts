import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import HomeHero from './components/HomeHero.vue'
import FeatureShowcase from './components/FeatureShowcase.vue'
import Layout from './Layout.vue'

import 'virtual:uno.css'
import './vars.css'
import './override.css'

export default {
  extends: DefaultTheme,
  Layout,
  enhanceApp({ app }) {
    app.component('HomeHero', HomeHero)

    app.component('FeatureShowcase', FeatureShowcase)
  }
} satisfies Theme
