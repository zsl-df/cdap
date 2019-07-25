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
import cloneDeep from 'lodash/cloneDeep';
import PropTypes from 'prop-types';
import { Input } from 'reactstrap';
import Select from 'react-select';
import { isNil, isEmpty, remove } from 'lodash';
import InfoTip from '../InfoTip';

require('./OperationSelector.scss');

class OperationSelector extends React.Component {
  availableOperations;
  operationConfigurations;
  operationMap;
  columns;
  constructor(props) {
    super(props);
    this.configPropList = [];
    this.operationMap = {};
    this.columns = [];
    this.state = {
      operations: []
    };
    this.onOperationChange = this.onOperationChange.bind(this);
    this.onValueUpdated = this.onValueUpdated.bind(this);
  }

  componentDidMount() {
    if (this.props.schema) {
      this.columns = this.props.schema.schemaColumns.map(column => {
        const value = column.columnName;
        const label = column.columnName;
        const type = column.columnType;
        return { value, label, type };
      });
    }
    this.availableOperations = cloneDeep(this.props.availableOperations);
    if (!isEmpty(this.availableOperations)) {
      this.availableOperations.forEach(element => {
        if (isNil(this.operationMap[element.paramName])) {
          this.operationMap[element.paramName] = {};
        }
        if (!isEmpty(element.subParams)) {
          element.subParams.forEach(subElement => {
            if (!isEmpty(subElement.defaultValue)) {
              this.operationMap[element.paramName][subElement.paramName] = subElement.defaultValue;
            }
          });
        }
      });
    }

    this.operationConfigurations = cloneDeep(this.props.operationConfigurations);
    if (!isNil(this.operationConfigurations)) {
      let operationList = [];
      for (let property in this.operationConfigurations) {
        if (property) {
          if (isNil(this.operationMap[property])) {
            this.operationMap[property] = {};
          }
          this.operationMap[property] = this.operationConfigurations[property];
          operationList.push(property);
        }
      }

      this.setState({
        operations: operationList
      });
    }
  }


  updateConfiguration() {
    const operationConfigurations = {};
    if (!isEmpty(this.state.operations) && this.state.operations.length > 0) {
      this.state.operations.forEach(element => {
        operationConfigurations[element] = {};
        if (!isNil(this.operationMap[element])) {
          operationConfigurations[element] = this.operationMap[element];
        }
      });
    }
    this.operationConfigurations = operationConfigurations;
    console.log("Update store with Operation -> ", this.operationConfigurations);
    this.props.updateOperationConfigurations(this.operationConfigurations);
  }

  onOperationChange(evt) {
    let operationList = [...this.state.operations];
    if (evt.target.checked) {
      operationList.push(evt.target.value);
    } else {
      operationList = remove(operationList, function (item) {
        return item !== evt.target.value;
      });
    }
    this.setState({
      operations: operationList
    });
    setTimeout(() => {
      this.updateConfiguration();
    });
  }

  onValueUpdated(parent, child, evt) {
    const value = evt.target.value;
    if (isNil(this.operationMap[parent])) {
      this.operationMap[parent] = {};
    }
    this.operationMap[parent][child] = value;
    this.updateConfiguration();
  }

  render() {
    return (
      <div className='operation-step-container'>
        {
          (this.props.availableOperations).map((item) => {
            return (
              <div className="config-container" key={item.paramName}>
                <div className="config-header-label">
                  <Input
                    type="checkbox"
                    value={item.paramName}
                    onChange={this.onOperationChange}
                    checked={this.state.operations.includes(item.paramName)}
                  />
                  <span>{item.displayName}</span>
                </div>

                {
                  this.state.operations.includes(item.paramName) &&
                  <div className="config-item-container">
                    {
                      (item.subParams).map(param => {
                        if (param.displayName == "Features") {
                          return (
                            <div className='list-row' key={param.paramName}>
                              <div className='name'>{param.displayName}
                                {
                                  param.isMandatory && <i className="fa fa-asterisk mandatory"></i>
                                }
                              </div>
                              <div className='colon'>:</div>
                              <Select className='value' isMulti = { true }
                                options={this.columns}/>
                              </div>
                          );
                        } else if (param.displayName == "Target Column") {
                          return (
                            <div className='list-row' key={param.paramName}>
                              <div className='name'>{param.displayName}
                                {
                                  param.isMandatory && <i className="fa fa-asterisk mandatory"></i>
                                }
                              </div>
                              <div className='colon'>:</div>
                              <Select className='value' 
                                options={this.columns}/>
                              </div>
                          );
                        } else {
                          return (
                            <div className='list-row' key={param.paramName}>
                              <div className='name'>{param.displayName}
                                {
                                  param.isMandatory && <i className="fa fa-asterisk mandatory"></i>
                                }
                              </div>
                              <div className='colon'>:</div>
                              <Input className='value' type="text" name="value"
                                placeholder={'Enter ' + param.displayName + ' value'}
                                defaultValue={isNil(this.operationMap[item.paramName][param.paramName]) ? '' : this.operationMap[item.paramName][param.paramName]}
                                onChange={this.onValueUpdated.bind(this, item.paramName, param.paramName)} />
                              {
                                param.description &&
                                <InfoTip id={param.paramName + "_InfoTip"} description={param.description}></InfoTip>
                              }
                            </div>
                          );
                        }
                      })
                    }
                  </div>
                }
              </div>
            );
          })
        }
      </div>
    );
  }
}
export default OperationSelector;
OperationSelector.propTypes = {
  availableOperations: PropTypes.array,
  operationConfigurations: PropTypes.any,
  updateOperationConfigurations: PropTypes.func,
  schema: PropTypes.any,
};
