/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import {objectQuery} from 'services/helpers';
import DataPrepBrowserStore from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import {ConnectionType} from 'components/DataPrepConnections/ConnectionType';
import FileBrowser from 'components/FileBrowser/index';

export default class ADLSBrowser extends FileBrowser {

  state = {
    contents: [],
    path: '',
    statePath: objectQuery(this.props, 'match', 'url') || '',
    loading: true,
    search: '',
    sort: 'name',
    sortOrder: 'asc',
    searchFocus: true
  };

  componentDidMount() {
    this.parsePath();
    this.browserStoreSubscription = DataPrepBrowserStore.subscribe(() => {
      let {adls, activeBrowser} = DataPrepBrowserStore.getState();
      if (activeBrowser.name !== ConnectionType.ADLS) {
        return;
      }

      if (this._isMounted) {
        this.setState({
          contents: adls.contents,
          loading: adls.loading,
          path: adls.path,
          search: adls.search
        });
      }
    });
  }
}
