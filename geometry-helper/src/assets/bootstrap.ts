import 'bootstrap/dist/css/bootstrap.css'
import 'bootstrap-vue/dist/bootstrap-vue.css'
import Vue from 'vue'

import { 
  PopoverPlugin,
  TabsPlugin,
  ToastPlugin,
  FormGroupPlugin,
  FormRadioPlugin 
} from 'bootstrap-vue'

const plugins = [
  PopoverPlugin,
  TabsPlugin,
  ToastPlugin,
  FormGroupPlugin,
  FormRadioPlugin
]

for (const plugin of plugins){
  Vue.use(plugin)
}