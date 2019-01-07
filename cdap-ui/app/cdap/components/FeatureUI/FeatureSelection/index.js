import React, { Component } from 'react';
import FilterContainer from './FilterContainer';
import './FeatureSelection.scss';
import GridHeader from './GridHeader';
import GridContainer from './GridContainer'
import { isNil } from 'lodash'
import { SERVER_IP,GET_PIPE_LINE_FILTERED_DATA } from '../config'

class FeatureSelection extends Component {
  //filterColumnList = [{id:1, name:'max'},{id:2, name:'min'},{id:3, name:'percentile'}, {id:4, name:'variance'}]

  constructor(props) {
    super(props)
    this.state = this.dataParser(this.props.pipeLineData);
  }

  dataParser = (data) => {
    const columDefs = [];
    const rowData = [];
    const columns = [];

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
          columDefs.push({ headerName: "featureName", field: "featureName", width: 250, checkboxSelection: true })
        }
        columns.forEach(element => {
          columDefs.push({ headerName: element.name, field: element.name })
        });
      }

      // generate grid data
      if (!isNil(item.featureStatistics)) {
        //let counter = 0;
        const rowObj = { featureName: item.featureName };
        columns.forEach(element => {
          rowObj[element.name] = item.featureStatistics[element.name]
        });

        rowData.push(rowObj);
      }
    });
    return {
      gridColumnDefs: columDefs,
      gridRowData: rowData,
      filterColumns: columns
    }
  }

  navigateToParentWindow = () => {
    this.props.nagivateToParent();
  }

  // componentDidMount() {
  //   if(!isNil(this.props.pipeLineData)){
  //     this.dataParser(this.props.pipeLineData)
  //     console.log("call feature selection mount");
  //   }
  // }

  applyFilter = (filterObj) => {
    const requestObject = this.requestGenerator(filterObj)
    this.getFilteredRecords(requestObject);
  }

  requestGenerator = (value) => {
    const filtersList = [];
    if (!isNil(value) && !isNil(value.filterItemList)) {
      value.filterItemList.forEach(element => {
        let low =0;
        let upper = 0;
        if(element.selectedFilterType.name === 'Range'){
          low = element.minValue.trim() == "" ? 0 : Number(element.minValue.trim());
          upper = element.maxValue.trim() == "" ? 0 : Number(element.maxValue.trim());
        }else{
          upper = element.minValue.trim() == "" ? 0 : Number(element.minValue.trim());
        }

        filtersList.push({
          filterType: element.selectedFilterType.name,
          statsName: element.selectedFilterColumn.name.replace(/\s/g, ""),
          lowerLimit: low,
          upperLimit: upper,
        })
      });
    }
    return {
      orderByStat: value.selectedOrderbyColumn.name.replace(/\s/g, ""),
      startPosition: value.minLimitValue.trim() == "" ? 0 : Number(value.minLimitValue.trim()),
      endPosition: value.maxLimitValue.trim() == "" ? 0 : Number(value.maxLimitValue.trim()),
      isComposite: true,
      compositeType: "OR",
      filterList: filtersList
    }
  }

  getFilteredRecords(requestObj) {

    const featureGenerationPipelineName = !isNil(this.props.selectedPipeline) ? this.props.selectedPipeline.pipelineName : "";

    let URL = SERVER_IP + GET_PIPE_LINE_FILTERED_DATA + featureGenerationPipelineName + '/features/filter';

    fetch(URL, {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(requestObj)
    }).then(res => res.json())
      .then(
        (result) => {
          if (isNil(result) || isNil(result["featureStatsList"])) {
            alert("Pipeline filter Data Error");
          } else {
            // this.setState({
            //   pipeLineData: result["featureStatsList"],
            //   data: result["featureStatsList"],
            //   selectedPipeline: this.currentPipeline,
            //   displayFeatureSelection: true
            // })
          }
        },
        (error) => {
          this.handleError(error, GET_PIPE_LINE_FILTERED);
        }
      )
  }

  render() {
    return (
      <div className="feature-selection-box">
        <div className="grid-box">
          {/* <button onClick={this.navigateToParentWindow}>Back</button> */}
          <GridHeader selectedPipeline={this.props.selectedPipeline} backnavigation={this.navigateToParentWindow}></GridHeader>
          <GridContainer gridColums={this.state.gridColumnDefs} rowData={this.state.gridRowData}></GridContainer>
        </div>
        <div className="filter-box">
          <FilterContainer filterColumns={this.state.filterColumns}
            applyFilter={this.applyFilter}></FilterContainer>
        </div>
      </div>

    )
  }
}

export default FeatureSelection;