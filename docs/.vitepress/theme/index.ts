import DefaultTheme from 'vitepress/theme';
import type { App } from 'vue';
import '@shikijs/vitepress-twoslash/style.css';
import TwoslashFloating from '@shikijs/vitepress-twoslash/client';
import './custom.css';
import 'virtual:uno.css';
import LandingFooter from '../components/LandingFooter.vue';
import LandingHero from '../components/LandingHero.vue';
import LandingPhilosophy from '../components/LandingPhilosophy.vue';
import Layout from '../components/Layouts.vue';
import Ray from '../components/Ray.vue';
import Showcase from '../components/Showcase.vue';

export default {
  extends: DefaultTheme,
  Layout,
  enhanceApp({ app }: { app: App }) {
    app.use(TwoslashFloating);
    app.component('Ray', Ray);
    app.component('Showcase', Showcase);
    app.component('LandingHero', LandingHero);
    app.component('LandingPhilosophy', LandingPhilosophy);
    app.component('LandingFooter', LandingFooter);
  },
};
