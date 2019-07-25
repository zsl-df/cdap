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
import { Input } from 'reactstrap';
import PropTypes from 'prop-types';
import { EDIT } from '../constant';
import isNil from 'lodash/isNil';

require('./DetailProvider.scss');

class DetailProvider extends React.Component {
  name;
  hadoopOutputPath;
  extraConfigurations;
  constructor(props) {
    super(props);
    this.state = {
      saveToHadoop: false
    };
  }

  componentWillMount() {
    this.extraConfigurations = this.props.extraConfigurations;
    this.hadoopOutputPath = isNil(this.extraConfigurations.hadoopOutputPath) ? "" :this.extraConfigurations.hadoopOutputPath;
    this.setState({
      saveToHadoop: (!isNil(this.extraConfigurations.saveToHadoop) && this.extraConfigurations.saveToHadoop == "Yes")  ? true : false
    });
  }

  onNameUpdated(event) {
    this.name = event.target.value;
    this.props.updatePipelineName(this.name);
  }

  onOutputPathUpdated(event) {
    this.hadoopOutputPath = event.target.value;
    this.extraConfigurations["saveToHadoop"]  = "Yes";
    this.extraConfigurations["hadoopOutputPath"]  = this.hadoopOutputPath;
    this.props.setExtraConfigurations(this.extraConfigurations);
  }

  onSaveToHadoopChange(evt) {
    if (evt.target.checked) {
      this.extraConfigurations["saveToHadoop"]  = "Yes";
      this.extraConfigurations["hadoopOutputPath"]  = this.hadoopOutputPath;
      this.props.setExtraConfigurations(this.extraConfigurations);
    } else {
      this.extraConfigurations["saveToHadoop"]  = "No";
      this.extraConfigurations["hadoopOutputPath"]  = null;
      this.props.setExtraConfigurations(this.extraConfigurations);
    }  

    this.setState({
      saveToHadoop: evt.target.checked
    });
  }

  render() {
    return (
      <div className="detail-step-container">
        <div className='field-row'>
          <div className='name'>Name
              <i className="fa fa-asterisk mandatory"></i>
          </div>
          <div className='colon'>:</div>
          <Input className='value' type="text" name="name" placeholder='name' readOnly={this.props.operationType == EDIT}
            defaultValue={this.props.pipelineName} onChange={this.onNameUpdated.bind(this)} />
        </div>
        <div className="config-header-label">
          <Input
            type="checkbox"
            onChange={this.onSaveToHadoopChange.bind(this)}
            checked={this.state.saveToHadoop}
          />
          <span>Save to Hadoop</span>
        </div> {
          this.state.saveToHadoop &&  
          <div className='field-row'>
            <div className='name'>Model Output Path
                <i className="fa fa-asterisk mandatory"></i>
            </div>
            <div className='colon'>:</div>
            <Input className='value' type="text" name="name" placeholder='name'
              defaultValue={this.hadoopOutputPath} onChange={this.onOutputPathUpdated.bind(this)} />
          </div>
        }
      </div>
    );
  }
}

export default DetailProvider;
DetailProvider.propTypes = {
  extraConfigurations: PropTypes.any,  
  setExtraConfigurations: PropTypes.func,
  updatePipelineName: PropTypes.func,
  operationType: PropTypes.string,
  pipelineName: PropTypes.string
};
