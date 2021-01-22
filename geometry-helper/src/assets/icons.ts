import { library } from "@fortawesome/fontawesome-svg-core";
import { FontAwesomeIcon } from "@fortawesome/vue-fontawesome";
import Vue from "vue"
console.log('IMPORTED ICONS')

// register these components globally
Vue.component("font-awesome-icon", FontAwesomeIcon);

import {
  faClipboardCheck 
} from '@fortawesome/free-solid-svg-icons'

library.add(faClipboardCheck)