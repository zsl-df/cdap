/*
 * Copyright © 2018 Cask Data, Inc.
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
package co.cask.cdap.featureengineer.service;

import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.featureengineer.FeatureEngineeringApp;
import co.cask.cdap.featureengineer.service.handler.ManualFeatureSelectionServiceHandler;

/**
 * @author bhupesh.goel
 *
 */
public class ManualFeatureSelectionService extends AbstractService {

    public static final String SERVICE_NAME = "ManualFeatureSelectionService";
    private final FeatureEngineeringApp.FeatureEngineeringConfig config;

    public ManualFeatureSelectionService(FeatureEngineeringApp.FeatureEngineeringConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {
        setName(SERVICE_NAME);
        setDescription("A service to manage selection of features manually.");
        addHandler(new ManualFeatureSelectionServiceHandler(config));
    }

}
