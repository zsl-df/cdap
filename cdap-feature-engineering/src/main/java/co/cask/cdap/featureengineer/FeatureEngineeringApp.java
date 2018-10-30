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
import co.cask.cdap.featureengineer.service.AutoFeatureEngineeringService;
import co.cask.cdap.featureengineer.service.DataPrepSchemaService;

/**
 * Feature Engineering Application.
 *
 */
public class FeatureEngineeringApp extends AbstractApplication<FeatureEngineeringApp.FeatureEngineeringConfig> {

	public static class FeatureEngineeringConfig extends Config {

		private String dataSchemaTable;

		private String pluginConfigTable;

		private String featureDAGTable;

		private String featureEngineeringConfigTable;
		
		private String pipelineDataSchemasTable;

		/**
		 * Set default values for the configuration variables.
		 */
		public FeatureEngineeringConfig() {
			this.dataSchemaTable = "schemaDataSet";
			this.pluginConfigTable = "pluginConfigDataSet";
			this.featureDAGTable = "featureDAGDataSet";
			this.featureEngineeringConfigTable = "featureEngineeringDataSet";
			this.pipelineDataSchemasTable = "pipelineDataSchemasDataSet";
		}

		/**
		 * Used only for unit testing.
		 */
		public FeatureEngineeringConfig(final String dataSchemaTable, final String pluginConfigTable,
				final String featureDAGTable, final String featureEngineeringConfigTable, final String pipelineDataSchemasTable) {
			this.dataSchemaTable = dataSchemaTable;
			this.pluginConfigTable = pluginConfigTable;
			this.featureDAGTable = featureDAGTable;
			this.featureEngineeringConfigTable = featureEngineeringConfigTable;
			this.pipelineDataSchemasTable = pipelineDataSchemasTable;
		}

		public String getDataSchemaTable() {
			return dataSchemaTable;
		}

		public String getPluginConfigTable() {
			return pluginConfigTable;
		}

		/**
		 * @return the featureDAGTable
		 */
		public String getFeatureDAGTable() {
			return featureDAGTable;
		}

		/**
		 * @return the featureEngineeringConfigTable
		 */
		public String getFeatureEngineeringConfigTable() {
			return featureEngineeringConfigTable;
		}

		public String getPipelineDataSchemasTable() {
			return pipelineDataSchemasTable;
		}

	}

	@Override
	public void configure() {
		FeatureEngineeringConfig config = getConfig();
		setName("FeatureEngineeringApp");
		setDescription("Application for Feature Engineering");
		createDataset(config.getDataSchemaTable(), KeyValueTable.class,
				DatasetProperties.builder().setDescription("Table to persist prepared data schema").build());
		createDataset(config.getPluginConfigTable(), KeyValueTable.class,
				DatasetProperties.builder().setDescription("Table to persist prepared wrangler plugin").build());
		createDataset(config.getFeatureDAGTable(), KeyValueTable.class,
				DatasetProperties.builder().setDescription("Table to persist Feature DAG").build());
		createDataset(config.getFeatureEngineeringConfigTable(), KeyValueTable.class,
				DatasetProperties.builder().setDescription("Table to persist feature engineering config").build());
		createDataset(config.getPipelineDataSchemasTable(), KeyValueTable.class,
				DatasetProperties.builder().setDescription("Table to persist pipeline data schemas").build());
		addService(new DataPrepSchemaService(config));
		addService(new AutoFeatureEngineeringService(config));
	}
}
