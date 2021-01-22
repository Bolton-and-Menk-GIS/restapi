/// <reference types="@types/arcgis-js-api" />

type SketchEvents = 
  | __esri.SketchCreateEvent
  | __esri.SketchUpdateEvent
  | __esri.SketchDeleteEvent 

type GeometryTypes = "point" | "multipoint" | "polyline" | "polygon" | "extent" | "mesh"