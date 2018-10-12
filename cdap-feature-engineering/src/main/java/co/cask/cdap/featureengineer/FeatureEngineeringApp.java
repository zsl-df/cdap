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

package co.cask.cdap.featureengineer;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.featureengineer.service.DataPrepSchemaService;

/**
 * Feature Engineering Application.
 *
 */
public class FeatureEngineeringApp extends AbstractApplication<FeatureEngineeringApp.FeatureEngineeringConfig> {

	public static class FeatureEngineeringConfig extends Config {

		private String dataSchemaTable;
		
		private String pluginConfigTable;

		/**
		 * Set default values for the configuration variables.
		 */
		public FeatureEngineeringConfig() {
			this.dataSchemaTable = "schemaDataSet";
			this.pluginConfigTable = "pluginConfigDataSet";
		}

		/**
		 * Used only for unit testing.
		 */
		public FeatureEngineeringConfig(String dataSchemaTable, String pluginConfigTable) {
			this.dataSchemaTable = dataSchemaTable;
			this.pluginConfigTable = pluginConfigTable;
		}

		public String getDataSchemaTable() {
			return dataSchemaTable;
		}
		
		public String getPluginConfigTable() {
			return pluginConfigTable;
		}

	}

	@Override
	public void configure() {
		FeatureEngineeringConfig config = getConfig();
		setName("FeatureEngineeringApp");
		setDescription("Application for Feature Engineering");
		createDataset(config.getDataSchemaTable(), KeyValueTable.class, DatasetProperties.builder()
				.setDescription("Table to persist prepared data schema").build());
		createDataset(config.getPluginConfigTable(), KeyValueTable.class, DatasetProperties.builder()
				.setDescription("Table to persist prepared wrangler plugin").build());
		addService(new DataPrepSchemaService(config));
	}
}
