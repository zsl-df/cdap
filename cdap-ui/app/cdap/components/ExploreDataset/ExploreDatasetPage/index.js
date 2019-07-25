/*
 * Copyright © 2017 Cask Data, Inc.
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

import React from 'react';
import NamespaceStore from 'services/NamespaceStore';
import PropTypes from 'prop-types';
import ExploreDatasetWizard from '../ExploreDatasetWizard';
import { getDefaultRequestHeader } from 'components/FeatureUI/util';
import EDADataServiceApi from '../dataService';
import { checkResponseError, getUpdatedConfigurationList } from '../Common/util';
import { GET_SINKS } from '../Common/constant';
import { IS_OFFLINE } from '../config';
import { sinks, configurations } from '../sampleData';

class ExploreDatasetPage extends React.Component {

  constructor(props) {
    super(props);
    this.toggleFeatureWizard = this.toggleFeatureWizard.bind(this);
    this.onWizardClose = this.onWizardClose.bind(this);
    if (IS_OFFLINE) {
      this.setAvailableSinks(sinks["configParamList"]);
      this.setEDAConfigurations(configurations["configParamList"]);
    } else {
      this.getSinkConfiguration();
      this.getEDAConfiguration();
    }
    this.state = {
      showExploreWizard: false,
      schema: [],
    };
  }

  componentWillMount() {
    const workspaceId = this.getURLParam("workspaceId");
    if (workspaceId) {
      const workspaceObj = JSON.parse(window.localStorage.getItem("Explore:" + workspaceId));
      if (workspaceObj) {
        const schema = {};
        schema["schemaName"] = workspaceObj.schema.name;
        schema["schemaColumns"] = workspaceObj.schema.fields
                                       .map(field => {
                                         const columnName = field.name;
                                         const columnType = field.type[0];
                                         return {columnName,columnType};
                                        });
        this.props.setSchema(schema);
      }
      this.toggleFeatureWizard();
    }
  }

  getSinkConfiguration() {
    EDADataServiceApi.availableSinks({
      namespace: NamespaceStore.getState().selectedNamespace,
    }, {}, getDefaultRequestHeader()).subscribe(
      result => {
        if (checkResponseError(result)) {
          this.handleError(result, GET_SINKS);
        } else {
          this.setAvailableSinks(result["configParamList"]);
        }
      },
      error => {
        this.handleError(error, GET_SINKS);
      }
    );
  }

  getEDAConfiguration() {
    EDADataServiceApi.configurationConfig({
      namespace: NamespaceStore.getState().selectedNamespace,
    }, {}, getDefaultRequestHeader()).subscribe(
      result => {
        if (checkResponseError(result)) {
          this.handleError(result, GET_SINKS);
        } else {
          this.setEDAConfigurations(result["configParamList"]);
        }
      },
      error => {
        this.handleError(error, GET_SINKS);
      }
    );
  }

  setAvailableSinks(response) {
    this.props.setAvailableSinks(response);
  }

  setEDAConfigurations(response) {
    const operations = [];
    const engineConfigs = [];
    if (response) {
      response.forEach(element => {
        if (element && element.hasOwnProperty("groupName")) {
          if (element["groupName"] == "eda") {
            operations.push(element);
          } else {
            engineConfigs.push(element);
          }
        }
      });
      this.props.setAvailableOperations(operations);
      this.props.setAvailableEngineConfigurations(engineConfigs);
      this.props.updateEngineConfigurations(getUpdatedConfigurationList(engineConfigs, []));
    }
  }

  fetchWizardData() {
  }

  getURLParam(key) {
    var q = window.location.search.match(new RegExp('[?&]' + key + '=([^&#]*)'));
    return q && q[1];
  }

  toggleFeatureWizard() {
    let open = !this.state.showExploreWizard;
    if (open) {
      this.fetchWizardData();
    }
    this.setState({
      showExploreWizard: open
    });
  }

  toggleDropDown() {
    this.setState(prevState => ({ dropdownOpen: !prevState.dropdownOpen }));
  }

  viewPipeline(pipeline) {
    let navigatePath = `${window.location.origin}/pipelines/ns/${NamespaceStore.getState().selectedNamespace}/view/${pipeline.pipelineName}`;
    window.location.href = navigatePath;
  }

  handleError(error, type) {
    console.log('error ==> ' + error + "| type => " + type);
  }

  savePipeline() {

  }

  onWizardClose() {
    this.setState({
      showExploreWizard: !this.state.showExploreWizard
    });
  }

  render() {
    return <div>
      <ExploreDatasetWizard showWizard={this.state.showExploreWizard}
        onClose={this.onWizardClose}
        onSubmit={this.savePipeline.bind(this)} />
    </div>;
  }

}
export default ExploreDatasetPage;
ExploreDatasetPage.propTypes = {
  setAvailableSinks: PropTypes.func,
  setAvailableOperations: PropTypes.func,
  setAvailableEngineConfigurations: PropTypes.func,
  updateEngineConfigurations: PropTypes.func,
  setSchema: PropTypes.func,
};
