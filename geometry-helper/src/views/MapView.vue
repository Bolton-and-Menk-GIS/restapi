<template>
  <div class="map-wrapper">
    <div id="viewDiv"></div>

    <div class="json-viewer-portal" ref="jsonViewer">
      <json-view 
        v-if="geometryAsJson"
        :geometry="geometryAsJson"
        :geometryType="geometryType"
      ></json-view>
    </div>
  </div>
</template>

<script lang="ts">
  /// <reference types="@types/arcgis-js-api" />
  import "@/types/esri"
  import { Component, Prop, Vue } from 'vue-property-decorator'
  import { loadModules } from 'esri-loader'

  @Component({
    components: {
      JsonView: ()=> import('@/components/JsonView.vue')
    }
  })
  export default class MapView extends Vue {
    
    map?: __esri.Map = undefined
    view?: __esri.MapView = undefined
    sketch?: __esri.Sketch = undefined
    expand?: __esri.Expand = undefined
    geometryAsJson: string = ''
    geometryType: GeometryTypes | undefined

    async mounted(){
      window.mp = this
      // load modules
      const [ Map, MapView, Sketch, Expand, Search, BasemapToggle, GraphicsLayer ] = await loadModules([
        "esri/Map",
        "esri/views/MapView",
        "esri/widgets/Sketch",
        "esri/widgets/Expand",
        "esri/widgets/Search",
        "esri/widgets/BasemapToggle",
        "esri/layers/GraphicsLayer"
      ])

      const layer = new GraphicsLayer()

      const map = new Map({
        basemap: "topo-vector",
        layers: [layer]
      })

      const view = new MapView({
        container: "viewDiv",
        map: map,
        zoom: 10,
        center: [-93, 45]
      })

      // create widgets
      const sketch = new Sketch({
        layer: layer,
        view: view,
        // graphic will be selected as soon as it is created
        creationMode: "update"
      })

      // disable lasso and rectangle selection tools
      sketch.visibleElements.selectionTools = {
        'lasso-selection': false,
        'rectangle-selection': false
      }
      
      // create expand widget for json viewer
      const expand = new Expand({
        content: this.$refs.jsonViewer,
        view
      })

      // set sketch events
      sketch.on('create', (event: __esri.SketchCreateEvent) => {
        if (event.state === 'start'){
          expand.collapse()

        }
        if (event.state === 'complete'){
          this.refreshJson(event)
        }
      })

      sketch.on('update', (event: __esri.SketchUpdateEvent) => {
        console.log('UPDATE? ', event)
        if (['start', 'active'].includes(event.state) && ['reshape', 'transform'].includes(event.tool)){
          this.refreshJson(event)
        }
        if (event.state === 'complete'){
          if (event.tool == 'transform'){
            expand.collapse()
          } else {
            this.refreshJson(event)
          }
        }
        if (event.tool === 'move'){
          this.refreshJson(event)
        }
      })

      sketch.on('delete', (e: __esri.SketchDeleteEvent) => {
        expand.collapse()
      })

      this.map = map
      this.view = view
      this.expand = expand
      this.sketch = sketch

      const search = new Search({ view })

      const basemapToggle = new BasemapToggle({
        view,
        nextBasemap: 'hybrid'
      })

      // add all widgets
      view.ui.add(search, { position: 'top-left', index: 0 })
      view.ui.add(basemapToggle, 'bottom-right')
      view.ui.add(sketch, "top-right")
      view.ui.add(expand, 'top-left')
    }

    /** 
     * update the json geometry in UI
     * @param {SketchEvents} event - the sketch event
     */
    refreshJson(event: SketchEvents){
      const graphic: __esri.Graphic = 'graphic' in event ? 
        (event as __esri.SketchCreateEvent).graphic :
        (event as __esri.SketchDeleteEvent | __esri.SketchUpdateEvent | __esri.SketchRedoEvent | __esri.SketchUndoEvent).graphics[0]
      
      console.log('graphic is? ', graphic)
      if (graphic){
        this.geometryAsJson = graphic.geometry.toJSON()
        this.geometryType = graphic.geometry.type
      }
      this.expand!.expand()
    }

  }

</script>

<style>
  .map-wrapper {
    height: calc(100vh);
    width: 100%;
    overflow-y: hidden;
  }

  #viewDiv {
    /* padding: 0;
    margin: 0; */
    height: 100%;
    width: 100%;
  }

  .json-viewer-portal {
    max-height: 685px;
  }
</style>