/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

import isObject from 'lodash/isObject';
import numeral from 'numeral';
import moment from 'moment';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import T from 'i18n-react';
import {compose} from 'redux';
import uuidV4 from 'uuid/v4';
import round from 'lodash/round';

/*
  Purpose: Query a json object or an array of json objects
  Return: Returns undefined if property is not defined(never set) and
          and a valid value (including null) if defined.
  Usage:
    var obj1 = [
      {
        p1: 'something',
        p2: {
          p21: 'angular',
          p22: 21,
          p23: {
            p231: 'ember',
            p232: null
          }
        },
        p3: 1296,
        p4: [1, 2, 3],
        p5: null
      },
      {
        p101: 'somethingelse'
      }
    ]
    1. query(obj1, 0, 'p1') => 'something'
    2. query(obj1, 0, 'p2', 'p22') => 21
    3. query(obj1, 0, 'p2', 'p32') => { p231: 'ember'}
    4. query(obj1, 0, 'notaproperty') => undefined
    5. query(obj1, 0, 'p2', 'p32', 'somethingelse') => undefined
    6. query(obj1, 1, 'p2', 'p32') => undefined
    7. query(obj1, 0, 'p2', 'p23', 'p232') => null
    8. query(obj1, 0, 'p5') => null
 */

function objectQuery(obj) {
  if (!isObject(obj)) {
    return null;
  }
  for (var i = 1; i < arguments.length; i++) {
    if (!isObject(obj)) {
      return undefined;
    }
    obj = obj[arguments[i]];
  }
  return obj;
}
export const HUMANREADABLESTORAGE = 'STORAGE';
export const HUMANREADABLESTORAGE_NODECIMAL = 'NODECIMAL';
export const HUMANREADABLE_DECIMAL = 'DECIMAL';
function humanReadableNumber(num, type) {
  if (typeof num !== 'number') {
    return num;
  }

  switch (type) {
    case HUMANREADABLESTORAGE:
      return convertBytesToHumanReadable(num);
    case HUMANREADABLESTORAGE_NODECIMAL:
      return convertBytesToHumanReadable(num, HUMANREADABLESTORAGE_NODECIMAL);
    case HUMANREADABLE_DECIMAL:
      return numeral(num).format('0,0[.]0000');
    default:
      return numeral(num).format('0,0');
  }

}
// FIXME: humanReadableDate(date, options = {isMilliseconds: false, shortForm: false}) would have been\
// more readable api. We should think about changing the function signature.
function humanReadableDate(date, isMilliseconds, shortForm = false) {
  if (!date) {
    return '--';
  }

  const format = shortForm ? 'MM-DD-YYYY': 'MM-DD-YYYY hh:mm:ss A';
  if (isMilliseconds) {
    return moment(date).format(format);
  }
  return (moment(date * 1000)).format(format);
}

const ONE_SECOND_MS = 1000;
const ONE_MIN_SECONDS = 60;
const ONE_HOUR_SECONDS = ONE_MIN_SECONDS * 60;
const ONE_DAY_SECONDS = ONE_HOUR_SECONDS * 24;
const ONE_WEEK_SECONDS = ONE_DAY_SECONDS * 7;
const ONE_MONTH_SECONDS = ONE_DAY_SECONDS * 30;
const ONE_YEAR_SECONDS = ONE_MONTH_SECONDS * 12;

function humanReadableDuration(timeInSeconds, shortForm = false) {
  if (typeof timeInSeconds !== 'number') {
    return timeInSeconds;
  }
  const pluralize = (number, label) => number > 1 ? `${label}s` : label;
  if (timeInSeconds < 60) {
    return `${Math.floor(timeInSeconds)} ${pluralize(timeInSeconds, T.translate('commons.secShortLabel'))}`;
  }
  if (timeInSeconds < ONE_HOUR_SECONDS) {
    let mins = Math.floor(timeInSeconds / ONE_MIN_SECONDS);
    let secs = Math.floor(timeInSeconds % ONE_MIN_SECONDS);
    return `${mins} ${pluralize(mins, 'min')} ${secs} secs`;
  }
  if (timeInSeconds < ONE_DAY_SECONDS) {
    let hours = Math.floor(timeInSeconds / ONE_HOUR_SECONDS);
    return (
      shortForm ?
        `${hours} ${pluralize(hours, 'hour')}`
      :
        `${hours} ${pluralize(hours, 'hour')} ${humanReadableDuration(timeInSeconds - (ONE_HOUR_SECONDS * hours))}`
    );
  }
  if (timeInSeconds < ONE_WEEK_SECONDS) {
    let days = Math.floor(timeInSeconds / ONE_DAY_SECONDS);
    return (
      shortForm ?
        `${days} ${pluralize(days, 'day')}`
      :
        `${days} ${pluralize(days, 'day')} ${humanReadableDuration(timeInSeconds - (ONE_DAY_SECONDS * days))}`
      );
  }
  // Hopefully we don't reach beyond this point.
  if (timeInSeconds < ONE_MONTH_SECONDS) {
    let weeks = Math.floor(timeInSeconds / ONE_WEEK_SECONDS);
    return (
      shortForm ?
        `${weeks} ${pluralize(weeks, 'week')}`
      :
        `${weeks} ${pluralize(weeks, 'week')} ${humanReadableDuration(timeInSeconds - (ONE_WEEK_SECONDS * weeks))}`
    );
  }
  if (timeInSeconds < ONE_YEAR_SECONDS) {
    let months = Math.floor(timeInSeconds / ONE_MONTH_SECONDS);
    return (
      shortForm ?
        `${months} ${pluralize(months, 'month')}`
      :
        `${months} ${pluralize(months, 'month')} ${humanReadableDuration(timeInSeconds - (ONE_MONTH_SECONDS * months))}`
    );
  }
}

function timeSinceCreated(timeInSeconds, shortForm) {
  return `${humanReadableDuration(timeInSeconds, shortForm)} ago`;
}

function contructUrl ({path}) {
  return [
    window.CDAP_CONFIG.sslEnabled? 'https://': 'http://',
    window.CDAP_CONFIG.cdap.routerServerUrl,
    ':',
    window.CDAP_CONFIG.sslEnabled? window.CDAP_CONFIG.cdap.routerSSLServerPort: window.CDAP_CONFIG.cdap.routerServerPort,
    '/v3',
    path
  ].join('');
}


function convertBytesToHumanReadable(bytes, type, includeSpace) {
  if (!bytes || typeof bytes !== 'number') {
    return bytes;
  }
  let format = includeSpace ? '0.00 b' : '0.00b';

  if (type === HUMANREADABLESTORAGE_NODECIMAL) {
    format = includeSpace ? '0 b' : '0b';
  }

  return numeral(bytes).format(format);
}

function isDescendant(parent, child) {
  var node = child;
  while (node != null) {
    if (node == parent) {
      return true;
    }
    node = node.parentNode;
  }
  return false;
}

function getArtifactNameAndVersion (nameWithVersion) {
  // core-plugins-3.4.0-SNAPSHOT.jar
  // extracts version from the jar file name. We then get the name of the artifact (that is from the beginning up to version beginning)
  // Fixed it to use a suffix pattern. Added `\\-` to detect versions from names such as `redshifttos3-action-plugin-1.0.0.json`
  if (isNil(nameWithVersion) || isEmpty(nameWithVersion)) {
    return {name: nameWithVersion, version: undefined};
  }
  let regExpRule = new RegExp('\\-(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?(?:[.\\-](.*))?$');
  let version = regExpRule.exec(nameWithVersion);
  if (!version) {
    return {name: nameWithVersion, version: undefined};
  }
  version = version[0].slice(1);
  let name = nameWithVersion.substr(0, nameWithVersion.indexOf(version) -1);
  return { version, name };
}


function insertAt(arr, index, element) {
  return [
    ...arr.slice(0, index + 1),
    element,
    ...arr.slice(index + 1, arr.length)
  ];
}

function removeAt(arr, index) {
  return [
    ...arr.slice(0, index),
    ...arr.slice(index + 1, arr.length)
  ];
}

function getIcon(entity) {
  switch (entity) {
    case 'application':
    case 'app':
      return 'icon-fist';
    case 'dataset':
      return 'icon-datasets';
    case 'stream':
      return 'icon-streams';
    default:
      return 'fa-exclamation-triangle';
  }
}

const defaultEventObject = {
  stopPropagation: () => {},
  nativeEvent: {
    stopImmediatePropagation: () => {}
  },
  preventDefault: () => {}
};

function preventPropagation(e = defaultEventObject) {
  e.stopPropagation();
  e.nativeEvent ? e.nativeEvent.stopImmediatePropagation() : e.stopImmediatePropagation();
  e.preventDefault();
}

function isNilOrEmptyString(value) {
  return isNil(value) || value === '';
}

function isNilOrEmpty(value) {
  return isNil(value) || isEmpty(value);
}

function isNumeric(value) {
  return !isNaN(parseFloat(value)) && isFinite(value);
}

function wholeArrayIsNumeric(values) {
  return values.reduce((prev, curr) => {
    return prev && isNumeric(curr);
  }, isNumeric(values[0]));
}

function requiredFieldsCompleted(state, requiredFields) {
  for (let i = 0; i < requiredFields.length; i++) {
    let requiredField = requiredFields[i];
    if (isNilOrEmptyString(state[requiredField])) {
      return false;
    }
  }

  return true;
}

const defaultAction = {
  action : '',
  payload : {}
};

const difference = (first, second) => {
  return first > second ? first - second : second - first;
};

const isPluginSink = (pluginType) => {
  return ['batchsink', 'realtimesink', 'sparksink', 'sparkdataframesink'].indexOf(pluginType) !== -1;
};

const isPluginSource = (pluginType) => {
  return ['batchsource', 'realtimesource', 'streamingsource', 'sparkdataframesource'].indexOf(pluginType) !== -1;
};

const isBatchPipeline = (pipelineType) => {
  return ['cdap-data-pipeline'].indexOf(pipelineType) !== -1;
};

const composeEnhancers = (storeTitle) =>
  typeof window === 'object' &&
  window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__ ?
    window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__({
      name: storeTitle
    }) : compose;

const reverseArrayWithoutMutating = (array) => {
  if (isNil(array)) {
    return [];
  }

  let newArray = [];
  for (let i = array.length - 1; i >= 0; i--) {
    newArray.push(array[i]);
  }
  return newArray;
};

const convertMapToKeyValuePairs = (map, addUniqueId = true) => {
  if (addUniqueId) {
    return Object.entries(map).map(([key, value]) => {
      return {
        key,
        value,
        uniqueId: 'id-' + uuidV4()
      };
    });
  }
  return Object.entries(map).map(([key, value]) => {
    return {
      key,
      value
    };
  });
};

const convertKeyValuePairsToMap = (keyValuePairs, ignoreNonNilValues = false) => {
  let map = {};
  keyValuePairs.forEach((currentPair) => {
    let isValidValue = ignoreNonNilValues || !isNilOrEmpty(currentPair.value);
    if (
      !isNilOrEmpty(currentPair.key) &&
      isValidValue
    ) {
      map[currentPair.key] = currentPair.value;
    }
  });
  return map;
};

const roundDecimalToNDigits = (num, digits = 2) => {
  let newNum = num;
  if (typeof num !== 'number') {
    newNum = parseFloat(num, 10);
  }
  if (isNaN(num)) {
    return num;
  }
  return round(newNum, digits);
};

/*
 *  This function is used to turn query parameters in the URL into a key-value object
 *  eg:
 *    URL: localhost:11011/some/path?key1=value1&key2=value2
 *
 *    returns:
 *    {
 *      key1: value1,
 *      key2: value2
 *    }
 */
const parseQueryString = () => {
  const queryStr = location.search.slice(1);

  if (queryStr.length === 0) { return null; }

  let queryObj = {};

  queryStr
    .split('&')
    .forEach((pair) => {
      const index = pair.indexOf('=');
      const key = pair.slice(0, index);
      const value = pair.slice(index + 1);

      queryObj[key] = value;
    });

  return queryObj;
};

export {
  objectQuery,
  convertBytesToHumanReadable,
  humanReadableNumber,
  humanReadableDuration,
  timeSinceCreated,
  isDescendant,
  getArtifactNameAndVersion,
  insertAt,
  removeAt,
  humanReadableDate,
  contructUrl,
  getIcon,
  preventPropagation,
  requiredFieldsCompleted,
  defaultAction,
  difference,
  isPluginSource,
  isPluginSink,
  isBatchPipeline,
  composeEnhancers,
  ONE_SECOND_MS,
  ONE_MIN_SECONDS,
  ONE_HOUR_SECONDS,
  ONE_DAY_SECONDS,
  ONE_WEEK_SECONDS,
  ONE_MONTH_SECONDS,
  ONE_YEAR_SECONDS,
  isNumeric,
  wholeArrayIsNumeric,
  reverseArrayWithoutMutating,
  convertMapToKeyValuePairs,
  convertKeyValuePairsToMap,
  isNilOrEmpty,
  roundDecimalToNDigits,
  parseQueryString
};
