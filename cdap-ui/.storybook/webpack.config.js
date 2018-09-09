/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/

const cdapconfig = require('../webpack.config.cdap');
const path = require("path");

module.exports = (baseConfig, env, defaultConfig) => {
  const config = defaultConfig;
  config.module.rules.pop();
  config.module.rules.pop();

  config.resolve.extensions.push(".ts", ".tsx", ".js");
  config.resolve.alias = cdapconfig.resolve.alias;

  config.module.rules[0].test = /\.(ts|tsx|js)$/;
  config.module.rules[0].query.presets = [
    "@babel/preset-env",
    "@babel/preset-react",
    "@babel/preset-typescript"
  ];

  config.module.rules.unshift({
    test: /\.(ts|tsx|js)$/,
    loader: require.resolve("ts-loader"),
    include: [path.resolve(__dirname, "../src")],
    options: {
      transpileOnly: true
    }
  }, {
    test: /\.scss$/,
    use: [
      'style-loader',
      'css-loader',
      'postcss-loader',
      'sass-loader'
    ]
  }, {
    test: /\.woff(2)?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
    use: [
      {
        loader: 'url-loader',
        options: {
          limit: 10000,
          mimetype: 'application/font-woff'
        }
      }
    ]
  }, {
    test: /\.(png|jpg|jpeg)?$/,
    use: 'url-loader'
  },
  {
    test: /\.(ttf|eot)(\?v=[0-9]\.[0-9]\.[0-9])?$/,
    use: 'url-loader'
  }, {
    test: /\.svg/,
    use: [
      {
        loader: 'svg-sprite-loader'
      }
    ]
  });

  // [ts-loader, babel-loader, ...]

  return config;
};