/*
 * Copyright © 2016 Cask Data, Inc.
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

import DataSourceConfigurer from 'services/datasource/DataSourceConfigurer';
import {apiCreatorAbsPath} from 'services/resource-helper';
import { Theme } from 'services/ThemeHelper';

let dataSrc = DataSourceConfigurer.getInstance();
let basepath = `${window.CDAP_CONFIG.marketUrl}`;



export function setMarketPath(marketType) {
  if (marketType===Theme.featureNames.hub) {
    basepath = `${window.CDAP_CONFIG.marketUrl}`;
  } else {
    basepath = `${window.CDAP_CONFIG.localMarketUrl}`;
  }
  MyMarketApi['list'] = apiCreatorAbsPath(dataSrc, 'GET', 'REQUEST', `${basepath}/packages.json`);
  MyMarketApi['getCategories'] = apiCreatorAbsPath(dataSrc, 'GET', 'REQUEST', `${basepath}/categories.json`);
  MyMarketApi['get'] = apiCreatorAbsPath(dataSrc, 'GET', 'REQUEST', `${basepath}/packages/:packageName/:version/spec.json`);
  MyMarketApi['getCategoryIcon'] =  (category) => {
    return `${basepath}/categories/${category}/icon.png`;
  };

  MyMarketApi['getIcon'] = (entity) => {
    return `${basepath}/packages/${entity.name}/${entity.version}/icon.png`;
  };

  MyMarketApi['getSampleData'] =  apiCreatorAbsPath(dataSrc, 'GET', 'REQUEST', `${basepath}/packages/:entityName/:entityVersion/:filename`);

}


export let MyMarketApi = {
  list: apiCreatorAbsPath(dataSrc, 'GET', 'REQUEST', `${basepath}/packages.json`),
  getCategories: apiCreatorAbsPath(dataSrc, 'GET', 'REQUEST', `${basepath}/categories.json`),
  get: apiCreatorAbsPath(dataSrc, 'GET', 'REQUEST', `${basepath}/packages/:packageName/:version/spec.json`),
  getCategoryIcon: (category) => {
    return `${basepath}/categories/${category}/icon.png`;
  },
  getIcon: (entity) => {
    return `${basepath}/packages/${entity.name}/${entity.version}/icon.png`;
  },
  getSampleData: apiCreatorAbsPath(dataSrc, 'GET', 'REQUEST', `${basepath}/packages/:entityName/:entityVersion/:filename`)
};
