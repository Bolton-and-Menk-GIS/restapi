
const path = require("path")

module.exports = {
  publicPath: "./",
  outputDir: path.resolve(__dirname, '../restapi/geometry-helper'),
  productionSourceMap: false,
  configureWebpack: config => {
  
    // drop console logs for production
    console.log('env: ', process.env.NODE_ENV)
    if (process.env.NODE_ENV === 'production') {
      if ('terserOptions' in config.optimization.minimizer[0].options || {}) {
        // eslint-disable-next-line no-console
        console.log('dropping console logs for production.');
        config.optimization.minimizer[0].options.terserOptions.compress.drop_console = true;
      }
    }
  }
};
