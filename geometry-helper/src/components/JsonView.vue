<template>
  <div class="json-viewer">
    <div class="json-header d-flex justify-content-between px-2">
      <h4>Geometry: {{ geometryType || "N/A" }}</h4>
      <div class="fa-btn bmi-green" @click="copyJson" v-b-popover.hover.right="'Copy To Clipboard'">
        <font-awesome-icon :icon="['fas', 'clipboard-check']"></font-awesome-icon>
      </div>
    </div>
    <b-tabs class="json-container" v-model="tabIndex">
      <b-tab title="esri-json" active>
        <highlightjs v-if="geometry" language="json" :code="jsonify(geometry)" style="height: 575px;"></highlightjs>
      </b-tab>

      <b-tab title="geojson">
        <highlightjs v-if="geometry" language="json" :code="jsonify(geojson)" style="height: 575px;"></highlightjs>
      </b-tab>
    </b-tabs>
    </div>
  </div>
</template>

<script lang="ts">
  import { Component, Prop, Emit, Vue } from 'vue-property-decorator'
  import { arcgisToGeoJSON } from '@esri/arcgis-to-geojson-utils'
  import 'highlight.js/styles/monokai-sublime.css'

  type JsonFormat =
    | "esri-json"
    | "geojson"

  @Component
  export default class JsonView extends Vue {

    @Prop({ required: true }) geometry: any
    @Prop({ required: true }) geometryType: GeometryTypes

    tabIndex = 0

    get jsonFormat(){
      return this.tabIndex === 0 ? 'esri-json': 'geojson'
    }

    get geojson(){
      return this.geometry ? arcgisToGeoJSON(this.geometry): {}
    }

    copyJson(){
      const text = JSON.stringify(this.jsonFormat === 'esri-json' ? this.geometry: this.geojson, null, 2)
      this.$copyText(text).then(()=> {
        this.$bvToast.toast(`Copied Geometry in "${this.jsonFormat}" format to Clipboard!`, {
          variant: 'success',
          title: 'Success',
          autoHideDelay: 5000,
          appendToast: true,
          toaster: 'b-toaster-bottom-left'
        })
      })
    }

    jsonify(obj: any){
      return JSON.stringify(obj, null, 2)
    }

  }

</script>

<style>
  .json-viewer {
    padding: 0.5rem;
    background-color: #f3f3f3;
    border: solid 1px gray;
    border-radius: 10px;
    z-index: 1000;
    max-height: 680px;
  }

  .json-container {
    overflow-y: auto;
    height: 650px;
    min-width: 325px;
  }

  code.hljs {
    overflow-y: auto !important;
  }

</style>