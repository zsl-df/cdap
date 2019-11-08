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

import isNil from 'lodash/isNil';
import cookie from 'react-cookie';
export const USE_REMOTE_SERVER = false;
export const REMOTE_IP = "http://192.168.156.36:11015";
export const RAF_ACCESS_TOKEN = "Agp1c3IwMQCs0NzvlVuswI/ClluwuMGmC0C1B07J4qgUqVTQQFN67O/6tK8ptiyE10qYYTgGXfxMPA==";

export const GET_MRDS_APP_DETAILS = "GET_MRDS_APP_DETAILS";
export const GET_MRDS_PROGRAM_STATUS= "GET_MRDS_PROGRAM_STATUS";
export const CHECK_MRDS_PROGRAM_INTERVAL = 1500;
export const CHECK_EXPERIMENT_INTERVAL = 30000;


export function getDefaultRequestHeader() {
    if (USE_REMOTE_SERVER) {
      return {
        "AccessToken": `Bearer ${RAF_ACCESS_TOKEN}`
      };
    } else {
      return (isNil(cookie.load('CDAP_Auth_Token'))) ? {} : { "AccessToken": `Bearer ${cookie.load('CDAP_Auth_Token')}` };
    }
  }
