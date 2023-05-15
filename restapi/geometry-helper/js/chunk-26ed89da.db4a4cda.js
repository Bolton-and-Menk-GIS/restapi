(window["webpackJsonp"]=window["webpackJsonp"]||[]).push([["chunk-26ed89da"],{"08b0":function(e,t,r){"use strict";r.r(t);var o=function(){var e=this,t=e.$createElement,r=e._self._c||t;return r("div",{staticClass:"json-viewer"},[r("div",{staticClass:"json-header d-flex justify-content-between px-2"},[r("h4",[e._v("Geometry: "+e._s(e.geometryType||"N/A"))]),r("div",{directives:[{name:"b-popover",rawName:"v-b-popover.hover.right",value:"Copy To Clipboard",expression:"'Copy To Clipboard'",modifiers:{hover:!0,right:!0}}],staticClass:"fa-btn bmi-green",on:{click:e.copyJson}},[r("font-awesome-icon",{attrs:{icon:["fas","clipboard-check"]}})],1)]),r("hr"),r("div",{staticClass:"geometry-projection"},[r("b-form-group",{scopedSlots:e._u([{key:"default",fn:function(t){var o=t.ariaDescribedby;return[r("b-form-radio-group",{attrs:{id:"radio-group-1",options:e.projectionOptions,"aria-describedby":o,name:"radio-options"},model:{value:e.projection,callback:function(t){e.projection=t},expression:"projection"}})]}}])})],1),r("b-tabs",{staticClass:"json-container",model:{value:e.tabIndex,callback:function(t){e.tabIndex=t},expression:"tabIndex"}},[r("b-tab",{attrs:{title:"esri-json",active:""}},[e.geometry?r("highlightjs",{staticStyle:{height:"575px"},attrs:{language:"json",code:e.jsonify(e.geom)}}):e._e()],1),r("b-tab",{attrs:{title:"geojson"}},[e.geometry?r("highlightjs",{staticStyle:{height:"575px"},attrs:{language:"json",code:e.jsonify(e.geojson)}}):e._e()],1)],1)],1)},n=[],i=r("d4ec"),a=r("bee2"),s=r("262e"),c=r("2caf"),u=r("9ab4"),p=r("1b40");function l(e,t){for(var r=0;r<e.length;r++)if(e[r]!==t[r])return!1;return!0}function f(e){return l(e[0],e[e.length-1])||e.push(e[0]),e}function y(e){var t,r=0,o=0,n=e.length,i=e[o];for(o;o<n-1;o++)t=e[o+1],r+=(t[0]-i[0])*(t[1]+i[1]),i=t;return r>=0}function g(e,t,r,o){var n=(o[0]-r[0])*(e[1]-r[1])-(o[1]-r[1])*(e[0]-r[0]),i=(t[0]-e[0])*(e[1]-r[1])-(t[1]-e[1])*(e[0]-r[0]),a=(o[1]-r[1])*(t[0]-e[0])-(o[0]-r[0])*(t[1]-e[1]);if(0!==a){var s=n/a,c=i/a;if(s>=0&&s<=1&&c>=0&&c<=1)return!0}return!1}function b(e,t){for(var r=0;r<e.length-1;r++)for(var o=0;o<t.length-1;o++)if(g(e[r],e[r+1],t[o],t[o+1]))return!0;return!1}function h(e,t){for(var r=!1,o=-1,n=e.length,i=n-1;++o<n;i=o)(e[o][1]<=t[1]&&t[1]<e[i][1]||e[i][1]<=t[1]&&t[1]<e[o][1])&&t[0]<(e[i][0]-e[o][0])*(t[1]-e[o][1])/(e[i][1]-e[o][1])+e[o][0]&&(r=!r);return r}function m(e,t){var r=b(e,t),o=h(e,t[0]);return!(r||!o)}function d(e){for(var t,r,o,n=[],i=[],a=0;a<e.length;a++){var s=f(e[a].slice(0));if(!(s.length<4))if(y(s)){var c=[s.slice().reverse()];n.push(c)}else i.push(s.slice().reverse())}var u=[];while(i.length){o=i.pop();var p=!1;for(t=n.length-1;t>=0;t--)if(r=n[t][0],m(r,o)){n[t].push(o),p=!0;break}p||u.push(o)}while(u.length){o=u.pop();var l=!1;for(t=n.length-1;t>=0;t--)if(r=n[t][0],b(r,o)){n[t].push(o),l=!0;break}l||n.push([o.reverse()])}return 1===n.length?{type:"Polygon",coordinates:n[0]}:{type:"MultiPolygon",coordinates:n}}function v(e){var t={};for(var r in e)e.hasOwnProperty(r)&&(t[r]=e[r]);return t}function j(e,t){for(var r=t?[t,"OBJECTID","FID"]:["OBJECTID","FID"],o=0;o<r.length;o++){var n=r[o];if(n in e&&("string"===typeof e[n]||"number"===typeof e[n]))return e[n]}throw Error("No valid id attribute found")}function x(e,t){var r={};if(e.features){r.type="FeatureCollection",r.features=[];for(var o=0;o<e.features.length;o++)r.features.push(x(e.features[o],t))}if("number"===typeof e.x&&"number"===typeof e.y&&(r.type="Point",r.coordinates=[e.x,e.y],"number"===typeof e.z&&r.coordinates.push(e.z)),e.points&&(r.type="MultiPoint",r.coordinates=e.points.slice(0)),e.paths&&(1===e.paths.length?(r.type="LineString",r.coordinates=e.paths[0].slice(0)):(r.type="MultiLineString",r.coordinates=e.paths.slice(0))),e.rings&&(r=d(e.rings.slice(0))),"number"===typeof e.xmin&&"number"===typeof e.ymin&&"number"===typeof e.xmax&&"number"===typeof e.ymax&&(r.type="Polygon",r.coordinates=[[[e.xmax,e.ymax],[e.xmin,e.ymax],[e.xmin,e.ymin],[e.xmax,e.ymin],[e.xmax,e.ymax]]]),(e.geometry||e.attributes)&&(r.type="Feature",r.geometry=e.geometry?x(e.geometry):null,r.properties=e.attributes?v(e.attributes):null,e.attributes))try{r.id=j(e.attributes,t)}catch(n){}return JSON.stringify(r.geometry)===JSON.stringify({})&&(r.geometry=null),e.spatialReference&&e.spatialReference.wkid&&e.spatialReference.wkid,r}r("6e2e");var O=function(e){Object(s["a"])(r,e);var t=Object(c["a"])(r);function r(){var e;return Object(i["a"])(this,r),e=t.apply(this,arguments),e.tabIndex=0,e.projection="wgs-1984",e.projectionOptions=[{text:"WGS 1984",value:"wgs-1984"},{text:"Web Mercator",value:"web-mercator"}],e}return Object(a["a"])(r,[{key:"copyJson",value:function(){var e=this,t=JSON.stringify("esri-json"===this.jsonFormat?this.geom:this.geojson,null,2);this.$copyText(t).then((function(){e.$bvToast.toast('Copied Geometry in "'.concat(e.jsonFormat,'" format to Clipboard!'),{variant:"success",title:"Success",autoHideDelay:5e3,appendToast:!0,toaster:"b-toaster-bottom-left"})}))}},{key:"jsonify",value:function(e){return JSON.stringify(e,null,2)}},{key:"jsonFormat",get:function(){return 0===this.tabIndex?"esri-json":"geojson"}},{key:"geom",get:function(){return"wgs-1984"===this.projection?this.geometryWGS84:this.geometry}},{key:"geojson",get:function(){return this.geom?x(this.geom):{}}}]),r}(p["c"]);Object(u["a"])([Object(p["b"])({required:!0})],O.prototype,"geometry",void 0),Object(u["a"])([Object(p["b"])({required:!0})],O.prototype,"geometryWGS84",void 0),Object(u["a"])([Object(p["b"])({required:!0})],O.prototype,"geometryType",void 0),O=Object(u["a"])([p["a"]],O);var w=O,k=w,C=(r("741c"),r("2877")),S=Object(C["a"])(k,o,n,!1,null,null,null);t["default"]=S.exports},"6e2e":function(e,t,r){},"741c":function(e,t,r){"use strict";r("e526")},e526:function(e,t,r){}}]);