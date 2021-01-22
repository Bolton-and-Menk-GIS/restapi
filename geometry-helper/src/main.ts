import Vue from 'vue'
import App from './App.vue'
import './registerServiceWorker'
import hljs from 'highlight.js/lib/core'
import json from 'highlight.js/lib/languages/json'
import { loadScript, loadCss } from 'esri-loader'
import VueClipboard from 'vue-clipboard2'
import '@/assets/bootstrap'
import '@/assets/icons'

VueClipboard.config.autoSetContainer = true

// register plugins
Vue.use(VueClipboard)
hljs.registerLanguage('json', json)
Vue.use(hljs.vuePlugin)

// preload ArcGIS JS API and css
loadScript()
loadCss()

Vue.config.productionTip = false

new Vue({
  render: h => h(App),
}).$mount('#app')
