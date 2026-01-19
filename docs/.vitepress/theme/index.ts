import DefaultTheme from 'vitepress/theme';
import type { App } from 'vue';
import 'virtual:uno.css';
import '@shikijs/vitepress-twoslash/style.css';
import TwoslashFloating from '@shikijs/vitepress-twoslash/client';
import './custom.css';
import EYN from '../components/EYN.vue';
import Footer from '../components/Footer.vue';
import Hero from '../components/Hero.vue';
import Layout from '../components/Layouts.vue';
import Ray from '../components/Ray.vue';
import Showcase from '../components/Showcase.vue';
import BenchmarkComparison from '../components/BenchmarkComparison.vue';

export default {
  extends: DefaultTheme,
  Layout,
  enhanceApp({ app }: { app: App }) {
    app.use(TwoslashFloating);
    app.component('Ray', Ray);
    app.component('Hero', Hero);
    app.component('EYN', EYN);
    app.component('Showcase', Showcase);
    app.component('BenchmarkComparison', BenchmarkComparison);
    app.component('Footer', Footer);
  },
};
