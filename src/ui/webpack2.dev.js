const path = require('path');
const merge = require('webpack-merge');
const common = require('./webpack.common.js');

module.exports = merge(common, {
  devtool: 'eval-source-map',
  devServer: {
    // Uncomment these to allow external (non-localhost) connections
    // host: '0.0.0.0',
    // disableHostCheck: true,

    port: 8079,
    contentBase: path.resolve(__dirname, 'dist'),
    publicPath: '/',
    stats: 'minimal',
    proxy: [
      {
        context: ['/api', '/config', '/login', '/logout', '/version'],
        target: 'http://localhost:2338/',
        ws: true,
      },
    ],
  },
});
