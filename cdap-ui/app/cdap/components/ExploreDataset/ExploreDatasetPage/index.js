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
import { checkResponseError, getUpdatedConfigurationList, getEDAObject } from '../Common/util';
import { GET_SINKS, SAVE_PIPELINE, GET_CONFIGURATION } from '../Common/constant';
import { IS_OFFLINE } from '../config';
import { sinks, configurations } from '../sampleData';
import { Observable } from 'rxjs/Observable';

class ExploreDatasetPage extends React.Component {
  originalSchema;
  pluginConfig;
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
    const workspaceId = window.localStorage.getItem("analyseWorkpaceId");
    if (workspaceId) {
      const workspaceObj = JSON.parse(window.localStorage.getItem(workspaceId));
      if (workspaceObj) {
        console.log(workspaceObj);
        this.originalSchema = workspaceObj.schema;
        this.pluginConfig =  workspaceObj.schema;
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
      window.localStorage.removeItem("analyseWorkpaceId");
      window.localStorage.removeItem(workspaceId);
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
          this.handleError(result, GET_CONFIGURATION);
        } else {
          this.setEDAConfigurations(result["configParamList"]);
        }
      },
      error => {
        this.handleError(error, GET_CONFIGURATION);
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

  toggleFeatureWizard() {
    let open = !this.state.showExploreWizard;
    this.setState({
      showExploreWizard: open
    });
  }

  toggleDropDown() {
    this.setState(prevState => ({ dropdownOpen: !prevState.dropdownOpen }));
  }

  startPipeline(pipeline) {
    EDADataServiceApi.startEDAPipeline({
      namespace: NamespaceStore.getState().selectedNamespace,
      pipeline: pipeline
    }, {}, getDefaultRequestHeader()).subscribe(
      result => {
        if (checkResponseError(result)) {
          this.handleError(result, "START");
        } else {
          this.viewPipeline(pipeline);
        }
      },
      error => {
        this.handleError(error, "START");
      }
    );
  }

  viewPipeline(pipeline) {
    let navigatePath = `${window.location.origin}/pipelines/ns/${NamespaceStore.getState().selectedNamespace}/view/${pipeline.pipelineName}`;
    window.location.href = navigatePath;
  }

  handleError(error, type) {
    console.log('error ==> ' + error + "| type => " + type);
  }

  savePipeline() {
    const edaPostObj = getEDAObject(this.props);
    if (edaPostObj) {
      edaPostObj["schema"] = JSON.stringify(this.originalSchema);
      edaPostObj["pluginConfig"] = JSON.stringify(this.pluginConfig);
    }
    console.log('EDA ==> ', edaPostObj);
    let fetchObserver = EDADataServiceApi.createEDAPipeline({
      namespace: NamespaceStore.getState().selectedNamespace,
      pipeline:  this.props.pipelineName
    }, edaPostObj, getDefaultRequestHeader());

    return Observable.create((observer) => {
      fetchObserver.subscribe(
        result => {
          if (checkResponseError(result)) {
            this.handleError(result, SAVE_PIPELINE);
            observer.error(result);
          } else {
            this.startPipeline(this.props.pipelineName);
            observer.next(result);
            observer.complete();
          }
        },
        err => {
          this.handleError(err, SAVE_PIPELINE);
          observer.error(err);
        }
      );
    });
  }

  onWizardClose() {
    this.setState({
      showExploreWizard: !this.state.showExploreWizard
    });
    // hack to allow re-open EDA
    const exploreDatasetURL = `${window.location.origin}/cdap/ns/${NamespaceStore.getState().selectedNamespace}/dataprep`;
    window.location.href = exploreDatasetURL;
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
  pipelineName: PropTypes.string,
};
