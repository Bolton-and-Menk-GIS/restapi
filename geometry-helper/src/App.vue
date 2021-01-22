<template>
  <div id="app">
    <map-view></map-view>
  </div>
</template>

<script lang="ts">
import { Component, Vue } from 'vue-property-decorator'
import MapView from '@/views/MapView'

@Component({
  components: {
    MapView
  },
})
export default class App extends Vue {}
</script>

<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  color: #2c3e50;
  /* margin-top: 60px; */
}

.fa-btn {
  cursor: pointer;
  font-size: 1.5rem;
}

.bmi-green {
  color: #006938;
}

.fa-btn:hover {
  cursor: pointer;
  transform:scale(1.3,1.3) !important;
  -webkit-transform:scale(1.3,1.3) !important;
  -moz-transform:scale(1.3,1.3) !important;
}
</style>
