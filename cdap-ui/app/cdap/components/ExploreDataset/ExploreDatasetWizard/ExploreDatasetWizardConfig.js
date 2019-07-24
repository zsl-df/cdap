/*
 * Copyright © 2016 Cask Data, Inc.
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
import DatasetOperationsStep from './DatasetOperationsStep';
import DatasetSinkStep from './DatasetSinkStep';
import DatasetDetailStep from './DatasetDetailStep';
import DatasetEngineConfigurationStep from './DatasetEngineConfigurationStep';


const ExploreDatasetWizardConfig = {
  steps: [
    {
      id: 'properties',
      shorttitle: 'EDA Config',
      title: 'Select Operation',
      description: '',
      content: (<DatasetOperationsStep />)
    },
    {
      id: 'sink',
      shorttitle: 'Sink Config',
      title: 'Set Sink Configuration',
      description: '',
      content: (<DatasetSinkStep />)
    },
    {
      id: 'detail',
      shorttitle: 'Pipeline',
      title: 'Set Details',
      description: '',
      content: (<DatasetDetailStep />)
    },
    {
      id: 'configuration',
      shorttitle: 'Engine Config',
      title: 'Set Configuration',
      description: '',
      content: (<DatasetEngineConfigurationStep />)
    }
  ]
};

export default ExploreDatasetWizardConfig;
