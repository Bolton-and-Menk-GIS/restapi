import 'bootstrap/dist/css/bootstrap.css'
import 'bootstrap-vue/dist/bootstrap-vue.css'
import Vue from 'vue'

import { 
  PopoverPlugin,
  TabsPlugin,
  ToastPlugin 
} from 'bootstrap-vue'

const plugins = [
  PopoverPlugin,
  TabsPlugin,
  ToastPlugin
]

for (const plugin of plugins){
  Vue.use(plugin)
}