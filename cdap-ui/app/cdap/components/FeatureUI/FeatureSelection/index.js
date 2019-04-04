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

import React, { Component } from 'react';
import FilterContainer from './FilterContainer';
import './FeatureSelection.scss';
/* import GridHeader from './GridHeader';
import GridContainer from './GridContainer'; */
import { isNil, cloneDeep } from 'lodash';
import {
  GET_PIPE_LINE_FILTERED,
  GET_FEATURE_CORRELAION,
  GET_PIPE_LINE_DATA,
  GET_PIPELINE
} from '../config';
import { TabContent, TabPane, Nav, NavItem, NavLink } from 'reactstrap';
import classnames from 'classnames';
import CorrelationContainer from './CorrelationContainer';
import FEDataServiceApi from '../feDataService';
import NamespaceStore from 'services/NamespaceStore';
import { checkResponseError } from '../util';
import SaveFeatureModal from './SaveFeatureModal';
import PropTypes from 'prop-types';
// import { Histogram, withScreenSize, BarSeries, XAxis, YAxis } from '@data-ui/histogram';
import { SuperChart } from '@superset-ui/chart';

class FeatureSelection extends Component {

  filterColumnDefs = [];
  filterGridRows = [];
  correlationColumnDefs = [];
  correlationGridRows = [];

  constructor(props) {
    super(props);
    const dataInfo = this.dataParser(this.props.pipeLineData);
    this.state = Object.assign({
      activeTab: "1", selectedFeatures: [],
      openSaveModal: false,
      enableSave:false,
      isDataLoading: false,
    }, dataInfo);


    this.storeGridInfo(true, dataInfo.gridColumnDefs, dataInfo.gridRowData);
    this.storeGridInfo(false, dataInfo.gridColumnDefs, dataInfo.gridRowData);
  }

  dataParser = (data) => {
    const columDefs = [];
    const rowData = [];
    const columns = [];
    const featureNames = [];

    let rowCount = 0;
    data.forEach(item => {
      if (columDefs.length <= 0) {
        // generate filter column
        if (!isNil(item.featureStatistics)) {
          let counter = 0;
          for (let key in item.featureStatistics) {
            columns.push({ id: counter, name: key });
            counter++;
          }
        }

        // generate column def
        if (!isNil(item.featureName)) {
          columDefs.push({ headerName: "Generated Feature", field: "featureName", width: 500, checkboxSelection: true, headerCheckboxSelection: true, headerCheckboxSelectionFilteredOnly: true, tooltipField:'featureName' });
        }
        columns.forEach(element => {
          columDefs.push({ headerName: element.name, field: element.name , resizable: true, filter:'agNumberColumnFilter'});
        });
      }

      // generate grid data
      if (!isNil(item.featureStatistics)) {
        // let counter = 0;
        const rowObj = { featureName: item.featureName };
        featureNames.push({ id: rowCount, name: item.featureName, selected: false });
        columns.forEach(element => {
          rowObj[element.name] = item.featureStatistics[element.name];
        });
        rowData.push(rowObj);
        rowCount++;
      }
    });

    return {
      gridColumnDefs: columDefs,
      gridRowData: rowData,
      filterColumns: columns,
      featureNames: featureNames
    };
  }

  storeGridInfo(isFilter, columDefs, rows) {
    if (isFilter) {
      this.filterColumnDefs = cloneDeep(columDefs);
      this.filterGridRows = cloneDeep(rows);
    } else {
      this.correlationColumnDefs = cloneDeep(columDefs);
      this.correlationGridRows = cloneDeep(rows);
    }
  }

  navigateToParentWindow = () => {
    this.props.nagivateToParent();
  }

  gridRowSelectionChange = (selectedRows) => {
    if (!isNil(selectedRows) && selectedRows.length > 0) {
      this.setState({ selectedFeatures: selectedRows, enableSave:true });

    } else {
      this.setState({ selectedFeatures: [], enableSave: false });
    }
  }

  applyFilter = (filterObj) => {
    const requestObject = this.requestGenerator(filterObj);
    this.getFilteredRecords(requestObject);
  }

  requestGenerator = (value) => {
    const filtersList = [];
    let result = {};
    if (!isNil(value) && !isNil(value.filterItemList)) {
      value.filterItemList.forEach(element => {
        let low = 0;
        let upper = 0;
        if (element.selectedFilterType.name === 'Range') {
          low = element.minValue.trim() == "" ? 0 : Number(element.minValue.trim());
          upper = element.maxValue.trim() == "" ? 0 : Number(element.maxValue.trim());
        } else {
          upper = element.minValue.trim() == "" ? 0 : Number(element.minValue.trim());
        }

        filtersList.push({
          filterType: element.selectedFilterType.name,
          statsName: element.selectedFilterColumn.name.replace(/\s/g, ""),
          lowerLimit: low,
          upperLimit: upper,
        });
      });
    }

    result = {
      startPosition: value.minLimitValue.trim() == "" ? 0 : Number(value.minLimitValue.trim()),
      endPosition: value.maxLimitValue.trim() == "" ? 0 : Number(value.maxLimitValue.trim()),
      isComposite: true,
      compositeType: value.selectedCompositeOption,
      filterList: filtersList
    };
    if (value.selectedOrderbyColumn.id != -1) {
      result['orderByStat'] = value.selectedOrderbyColumn.name.replace(/\s/g, "");
    }

    return result;
  }

  getFilteredRecords(requestObj) {
    const featureGenerationPipelineName = !isNil(this.props.selectedPipeline) ? this.props.selectedPipeline.pipelineName : "";
    this.setState ({
      isDataLoading: true
    });
    FEDataServiceApi.pipelineFilteredData(
      {
        namespace: NamespaceStore.getState().selectedNamespace,
        pipeline: featureGenerationPipelineName,
      }, requestObj).subscribe(
        result => {
          if (checkResponseError(result) || isNil(result["featureStatsList"])) {
            this.handleError(result, GET_PIPE_LINE_FILTERED);
            this.setState ({
              isDataLoading: false,
              gridRowData: []
            });
          } else {
            const parsedResult = this.dataParser(result["featureStatsList"]);
            this.storeGridInfo(true, parsedResult.gridColumnDefs, parsedResult.gridRowData);
            this.setState({
              isDataLoading: false,
              gridRowData: parsedResult.gridRowData
            });
          }
        },
        error => {
          this.handleError(error, GET_PIPE_LINE_FILTERED);
          this.setState ({
            isDataLoading: false,
            gridRowData: []
          });
        }
      );
  }

  toggle(tab) {
    if (this.state.activeTab !== tab) {
      if (tab == '1') {
        this.setState({ gridColumnDefs: this.filterColumnDefs, gridRowData: this.filterGridRows, activeTab: tab });
      } else {
        this.setState({ gridColumnDefs: this.correlationColumnDefs, gridRowData: this.correlationGridRows, activeTab: tab });
      }
    }
  }

  applyCorrelation = (value) => {
    const featureGenerationPipelineName = !isNil(this.props.selectedPipeline) ? this.props.selectedPipeline.pipelineName : "";
    const selectedFeatures = [value.selectedfeatures.name];
    this.setState ({
      isDataLoading: true
    });
    FEDataServiceApi.featureCorrelationData(
      {
        namespace: NamespaceStore.getState().selectedNamespace,
        pipeline: featureGenerationPipelineName,
        coefficientType: value.coefficientType.name
      }, selectedFeatures).subscribe(
        result => {
          if (checkResponseError(result) || isNil(result["featureCorrelationScores"])) {
            this.handleError(result, GET_FEATURE_CORRELAION);
            this.setState ({
              isDataLoading: false,
              gridRowData: [],
            });
          } else {
            const parsedResult = this.praseCorrelation(result["featureCorrelationScores"]);
            this.storeGridInfo(false, parsedResult.gridColumnDefs, parsedResult.gridRowData);
            this.setState({
              isDataLoading: false,
              gridColumnDefs: parsedResult.gridColumnDefs,
              gridRowData: parsedResult.gridRowData });
          }
        },
        error => {
          this.setState ({
            isDataLoading: false,
            gridRowData: [],
          });
          this.handleError(error, GET_FEATURE_CORRELAION);
        }
      );
  }

  clearCorrelation = () => {
    this.onFeatureSelection(this.props.selectedPipeline);

  }


  handleError(error, type) {
    console.log('error ==> '+ error + "| type => " + type);
  }

  praseCorrelation = (value) => {
    const columDefs = [];
    const rowData = [];
    // generate column def\featureCorrelationScores
    if (!isNil(value) && value.length > 0) {
      const item = value[0]['featureCorrelationScores'];
      columDefs.push({ headerName: "Generated Feature", field: "featureName", width: 700, checkboxSelection: true,headerCheckboxSelection: true, headerCheckboxSelectionFilteredOnly: true, tooltipField:'featureName'  });
      columDefs.push({ headerName: "Value", field: "value", filter:'agNumberColumnFilter' });

      if (!isNil(item)) {
        for (let key in item) {
          rowData.push({ featureName: key, value: item[key] });
        }
      }
    }
    return {
      gridColumnDefs: columDefs,
      gridRowData: rowData
    };
  }

  onSaveClick =()=>{
    this.setState({openSaveModal:true});
  }

  onSaveModalClose = () => {
    this.setState({openSaveModal:false});
  }

  onFeatureSelection(pipeline, isFilter=false) {
    this.currentPipeline = pipeline;
    this.setState ({
      isDataLoading: true
    });
    FEDataServiceApi.pipelineData({
      namespace: NamespaceStore.getState().selectedNamespace,
      pipeline: pipeline.pipelineName
    }).subscribe(
      result => {
        if (checkResponseError(result) || isNil(result["featureStatsList"])) {
          this.handleError(result, GET_PIPE_LINE_DATA);
          this.setState ({
            isDataLoading: false,
            gridRowData: []
          });
        } else {
          const data = this.dataParser(result["featureStatsList"]);
          if (!isFilter) {
            this.storeGridInfo(false, data.gridColumnDefs, data.gridRowData);
            this.setState({
              gridColumnDefs:data.gridColumnDefs,
              gridRowData: data.gridRowData,
              isDataLoading: false
           });
          }
        }
      },
      error => {
        this.handleError(error, GET_PIPELINE);
        this.setState ({
          isDataLoading: false,
          gridRowData: []
        });
      }
    );
  }


  render() {
    const arr = this.state.gridRowData;

   /* const ResponsiveHistogram = withScreenSize(({ screenWidth, ...rest }) => (
      <Histogram
        width={Math.min(1000, screenWidth / 1.3)}
        height={Math.min(1000 / 1.8, screenWidth / 1.3 / 1.8)}
        {...rest}
      >
      </Histogram>
    ));
    const dataUI = (<ResponsiveHistogram
      ariaLabel="My histogram of ..."
      orientation="vertical"
      cumulative={false}
      normalized={true}
      binCount={25}
      valueAccessor={datum => datum['Num of Non Zeros']}
      binType="numeric"
      >

      <BarSeries
        fillOpacity={0.15}
        rawData={arr}
      />
      <XAxis />
      <YAxis />
    </ResponsiveHistogram>);
    console.log(dataUI); */
      const data = [
        {
          key: 'Entrance exam',
          values: [
            0.87,
            0.944,
            1.0,
            0.879,
            0.69,
            0.667,
            0.794,
            0.838,
            0.875,
            0.385,
            0.968,
            0.804,
            1.0,
            0.943,
            0.96,
            0.333,
            0.5,
            0.929,
            0.863,
            0.75,
            0.957,
            0.914,
            1.0,
            0.909,
            0.742,
            0.964,
            0.25,
            0.75,
            0.5,
            0.867,
            0.909,
            0.333,
            0.867,
            0.952,
            0.857,
            0.949,
            0.857,
            0.333,
            0.8,
            0.707,
            0.833,
            0.75,
            0.88,
            0.771,
            1.0,
            1.0,
            0.769,
            1.0,
            0.769,
            0.622,
            0.909,
            0.725,
            0.951,
            1.0,
          ],
        },
      ];

    return (
      <div className="feature-selection-box">
        <div className="grid-box">
        <SuperChart
          chartType="histogram"
          chartProps={{
            formData: {
              colorScheme: 'd3Category10',
              globalOpacity: 1,
              linkLength: 15, // binCount
              normalized: false,
              xAxisLabel: 'Score',
              yAxisLabel: 'Count',
            },
            height: 400,
            payload: { data },
            width: 400,
          }}
        />

        </div>
        <div className="filter-box">
          <Nav tabs className="tab-header">
            <NavItem>
              <NavLink
                className={classnames({ active: this.state.activeTab === '1' })}
                onClick={() => { this.toggle('1'); }}>
                Filter
              </NavLink>
            </NavItem>
            <NavItem>
              <NavLink
                className={classnames({ active: this.state.activeTab === '2' })}
                onClick={() => { this.toggle('2'); }}>
                Correlation
              </NavLink>
            </NavItem>
          </Nav>
          <TabContent activeTab={this.state.activeTab} className="tab-content">
            <TabPane tabId="1" className="tab-pane">
              <FilterContainer filterColumns={this.state.filterColumns}
                applyFilter={this.applyFilter}></FilterContainer>
            </TabPane>
            <TabPane tabId="2" className="tab-pane">
              <CorrelationContainer applyCorrelation={this.applyCorrelation} featureNames={this.state.featureNames}
              onClear={this.clearCorrelation}></CorrelationContainer>
            </TabPane>
          </TabContent>
        </div>

        <SaveFeatureModal open={this.state.openSaveModal} message={this.state.alertMessage}
          onClose={this.onSaveModalClose} selectedPipeline={this.props.selectedPipeline}
          selectedFeatures={this.state.selectedFeatures}/>
      </div>
    );
  }
}

export default FeatureSelection;
FeatureSelection.propTypes = {
  pipeLineData: PropTypes.array,
  nagivateToParent: PropTypes.func,
  selectedPipeline: PropTypes.object
};
