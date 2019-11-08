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

import React, { Component } from 'react';
import { AgGridReact } from 'ag-grid-react';
import PropTypes from 'prop-types';
import 'ag-grid-community/dist/styles/ag-theme-material.css';
import { EXP_COLUMN_DEF } from './config';
import { isNil } from 'lodash';
import { HEADER_HEIGHT, ROW_HEIGHT } from 'components/MRDS/ModelManagementStore/constants';
import ExperimentDetail from '../ExperimentDetail';
import MRDSServiceApi from 'components/MRDS/mrdsServiceApi';
import NamespaceStore from 'services/NamespaceStore';
import { getDefaultRequestHeader, GET_MRDS_APP_DETAILS, GET_MRDS_PROGRAM_STATUS, CHECK_MRDS_PROGRAM_INTERVAL, CHECK_EXPERIMENT_INTERVAL } from '../../config';
import { isNilOrEmpty } from 'services/helpers';

require('./Landing.scss');

class LandingPage extends Component {
  gridApi;
  gridColumnApi;
  programs;
  runningProgramsId;
  checkStatusInterval;
  checkExperimentInterval;
  baseUrl = "";

  constructor(props) {
    super(props);
    this.state = {
      columnDefs: EXP_COLUMN_DEF,
      experiments: [],
      numberOfExperiment: 0,
      context: { componentParent: this },
      isRouteToExperimentDetail: false,
      isFeatureColumnModal: false,
      featuredColumns: '',
      rowHeight: ROW_HEIGHT,
      headerHeight: HEADER_HEIGHT,
      experimentDetail:undefined,
      enablingInProgress: false,
      isMRDSServiceRunning: false
    };
  }


  componentWillMount() {
    this.fetchAppDetails();
    // this.getExperimentDetails();
  }

  handleError(error, type) {
    console.log('error ==> ' + error + "| type => " + type);
  }

  fetchAppDetails() {
    MRDSServiceApi.appDetails(
      {
        namespace: NamespaceStore.getState().selectedNamespace
      }, {}, getDefaultRequestHeader()).subscribe(
        result => {
          if (isNil(result) || isNil(result.programs)) {
            this.handleError(result, GET_MRDS_APP_DETAILS);
          } else {
            this.programs = result.programs.map((program) => {
              return {"appId": program.app,"programType":program.type,"programId":program.name};
            });
            console.log(this.programs);
            this.fetchProgramStatus();
          }
        },
        error => {
          this.handleError(error, GET_MRDS_APP_DETAILS);
        }
      );
  }

  fetchProgramStatus() {
    this.runningProgramsId = new Set();
    MRDSServiceApi.status(
      {
        namespace: NamespaceStore.getState().selectedNamespace
      }, this.programs, getDefaultRequestHeader()).subscribe(
        result => {
          if (isNilOrEmpty(result)) {
            this.handleError(result, GET_MRDS_PROGRAM_STATUS);
          } else {
            result.forEach((program) => {
              if (program.status == "RUNNING") {
                this.runningProgramsId.add(program.programId);
              }
            });
            const isMRDSServiceRunning = this.runningProgramsId.size > 0;
            this.setState({
              isMRDSServiceRunning: isMRDSServiceRunning,
            });
            if (isMRDSServiceRunning) {
              clearInterval(this.checkStatusInterval);
              this.getExperimentDetails();
              this.startExperimentInterval();
            }
             console.log(this.runningProgramsId);
            }
        },
        error => {
          this.handleError(error, GET_MRDS_PROGRAM_STATUS);
        }
      );
  }

  startPrograms() {
    clearInterval(this.checkStatusInterval);
    this.setState({
      enablingInProgress: true
    });
    MRDSServiceApi.start(
      {
        namespace: NamespaceStore.getState().selectedNamespace
      }, this.programs.filter((program) => {
        return !this.runningProgramsId.has(program.programId);
      }), getDefaultRequestHeader()).subscribe(
        result => {
          if (isNilOrEmpty(result)) {
            this.handleError(result, GET_MRDS_PROGRAM_STATUS);
          } else {
            this.checkStatusInterval = setInterval(() => {
              this.fetchProgramStatus();
            }, CHECK_MRDS_PROGRAM_INTERVAL);
          }
        },
        error => {
          this.handleError(error, GET_MRDS_PROGRAM_STATUS);
        }
      );
  }


  startExperimentInterval() {
    clearInterval(this.checkExperimentInterval);
    this.checkExperimentInterval = setInterval(() => {
      this.getExperimentDetails();
    }, CHECK_EXPERIMENT_INTERVAL);
  }

  getExperimentDetails() {
    MRDSServiceApi.fetchExperimentsDetails({
      namespace: NamespaceStore.getState().selectedNamespace,
    }, {}, getDefaultRequestHeader()).subscribe(
      result => {
        if (!isNil(result) && result.length > 0) {
          this.setState({
            experiments: result
          });
        } else {
          this.setState({
            experiments: []
          });
        }
      },
      error => {
        this.handleError(error, "Getting Experiment Details");
      }
    );
  }

  onGridReady = params => {
    this.gridApi = params.api;
    this.gridColumnApi = params.columnApi;

    window.addEventListener("resize", function() {
      setTimeout(function() {
        params.api.sizeColumnsToFit();
      });
    });
    this.gridApi.sizeColumnsToFit();
  }

  gridCellClick(event) {
    if (event.colDef.field === 'experimentName') {
      this.setState({
        isRouteToExperimentDetail: true,
        experimentDetail: event.data
      });
    }
  }

  showGridContent() {
    this.setState({
      isRouteToExperimentDetail: false,
      experimentDetail: undefined
    });
  }

  render() {
    setTimeout(() => {
      if (this.gridApi) {
        this.gridApi.sizeColumnsToFit();
      }
    }, 500);
    if (!this.state.isMRDSServiceRunning ) {
      console.log("Enabling In Progress ", this.state.enablingInProgress);
      return (<div className = "mrds-fullscreen-layout">
          <button className="mrds-button" disabled = { this.state.enablingInProgress } onClick={this.startPrograms.bind(this)}>Enable Model Management</button>
      </div>);
    }

    if (this.state.isRouteToExperimentDetail) {
      return <ExperimentDetail detail = { this.state.experimentDetail }
      navigateToParent = { this.showGridContent.bind(this) }/>;
    } else {
      return (
        <div className='landing-page'>
          <div className="ag-theme-material landing-page-grid">
            <AgGridReact
              columnDefs={this.state.columnDefs}
              rowData={this.state.experiments}
              context={this.state.context}
              frameworkComponents={this.state.frameworkComponents}
              onGridReady={this.onGridReady}
              rowHeight={this.state.rowHeight}
              headerHeight={this.state.headerHeight}
              onCellClicked={this.gridCellClick.bind(this)}
            >
            </AgGridReact>
          </div>
        </div>
      );
    }
  }
}
export default LandingPage;
LandingPage.propTypes = {
  data: PropTypes.any,
};
