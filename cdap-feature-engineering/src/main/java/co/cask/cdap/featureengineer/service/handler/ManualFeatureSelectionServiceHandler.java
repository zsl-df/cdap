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
package co.cask.cdap.featureengineer.service.handler;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.common.enums.FeatureSTATS;
import co.cask.cdap.feature.selection.CDAPSubDagGenerator;
import co.cask.cdap.featureengineer.FeatureEngineeringApp.FeatureEngineeringConfig;
import co.cask.cdap.featureengineer.RequestExtractor;
import co.cask.cdap.featureengineer.pipeline.pojo.CDAPPipelineInfo;
import co.cask.cdap.featureengineer.request.pojo.DataSchemaNameList;
import co.cask.cdap.featureengineer.response.pojo.FeatureStats;
import co.cask.cdap.featureengineer.response.pojo.SelectedFeatureStats;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchema;
import co.cask.cdap.featureengineer.proto.FeatureGenerationRequest;
import co.cask.cdap.featureengineer.proto.FeatureSelectionRequest;
import co.cask.cdap.featureengineer.utils.JSONInputParser;

/**
 * @author bhupesh.goel
 *
 */
public class ManualFeatureSelectionServiceHandler extends BaseServiceHandler {

	private static final Logger LOG = LoggerFactory.getLogger(ManualFeatureSelectionServiceHandler.class);

	@Property
	private final String dataSchemaTableName;
	@Property
	private final String pluginConfigTableName;
	@Property
	private final String featureDAGTableName;
	@Property
	private final String featureEngineeringConfigTableName;
	@Property
	private final String pipelineDataSchemasTableName;

	private KeyValueTable dataSchemaTable;
	private KeyValueTable pluginConfigTable;
	private KeyValueTable featureDAGTable;
	private KeyValueTable featureEngineeringConfigTable;
	private KeyValueTable pipelineDataSchemasTable;

	private HttpServiceContext context;

	/**
	 * @param config
	 * 
	 */
	public ManualFeatureSelectionServiceHandler(FeatureEngineeringConfig config) {
		this.dataSchemaTableName = config.getDataSchemaTable();
		this.pluginConfigTableName = config.getPluginConfigTable();
		this.featureDAGTableName = config.getFeatureDAGTable();
		this.featureEngineeringConfigTableName = config.getFeatureEngineeringConfigTable();
		this.pipelineDataSchemasTableName = config.getPipelineDataSchemasTable();
	}

	@Override
	public void initialize(HttpServiceContext context) throws Exception {
		super.initialize(context);
		this.dataSchemaTable = context.getDataset(dataSchemaTableName);
		this.pluginConfigTable = context.getDataset(pluginConfigTableName);
		this.featureDAGTable = context.getDataset(featureDAGTableName);
		this.featureEngineeringConfigTable = context.getDataset(featureEngineeringConfigTableName);
		this.pipelineDataSchemasTable = context.getDataset(pipelineDataSchemasTableName);
		this.context = context;
	}

	@POST
	@Path("featureengineering/{pipelineName}/features/selected/create/pipeline")
	public void generateSelectedFeaturesPipeline(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("pipelineName") String featureGenerationPipelineName) {
		Map<String, NullableSchema> inputDataschemaMap = new HashMap<>();
		Map<String, CDAPPipelineInfo> wranglerPluginConfigMap = new HashMap<>();
		try {
			String dataSchemaNames = readCDAPKeyValueTable(pipelineDataSchemasTable, featureGenerationPipelineName);
			DataSchemaNameList schemaList = (DataSchemaNameList) JSONInputParser.convertToObject(dataSchemaNames,
					DataSchemaNameList.class);

			FeatureSelectionRequest featureSelectionRequest = new RequestExtractor(request).getContent("UTF-8",
					FeatureSelectionRequest.class);
			String featureEngineeringPipelineName = featureSelectionRequest.getFeatureEngineeringPipeline();

			inputDataschemaMap = getSchemaMap(schemaList.getDataSchemaName(), dataSchemaTable);
			wranglerPluginConfigMap = getWranglerPluginConfigMap(schemaList.getDataSchemaName(), pluginConfigTable);
			String hostAndPort[] = getHostAndPort(request);
			String featuresConfig = readCDAPKeyValueTable(featureEngineeringConfigTable,
					featureEngineeringPipelineName);
			FeatureGenerationRequest featureGenerationRequest = (FeatureGenerationRequest) JSONInputParser
					.convertToObject(featuresConfig, FeatureGenerationRequest.class);
			String featureDag = readCDAPKeyValueTable(featureDAGTable, featureEngineeringPipelineName);

			new CDAPSubDagGenerator(featureDag, inputDataschemaMap, wranglerPluginConfigMap, featureGenerationRequest,
					hostAndPort).triggerCDAPPipelineGeneration(featureSelectionRequest.getSelectedFeatures(),
							featureSelectionRequest.getFeatureSelectionPipeline());

			success(responder, "Successfully Generated CDAP Pipeline for selected Feature for data schemas "
					+ inputDataschemaMap.keySet());
			LOG.info("Successfully Generated CDAP Pipeline for selected Feature for data schemas "
					+ inputDataschemaMap.keySet());
		} catch (Exception e) {
			error(responder, "Failed to generate cdap pipeline for selected features for data schemas "
					+ inputDataschemaMap.keySet() + " with error message " + e.getMessage());
			LOG.error("Failed to generate cdap pipeline for selected features for data schemas "
					+ inputDataschemaMap.keySet(), e);
		}
	}

	@GET
	@Path("featureengineering/features/stats/get")
	public void getFeatureStats(HttpServiceRequest request, HttpServiceResponder responder,
			@QueryParam("pipelineName") String featureGenerationPipelineName) {
		Table featureStatsDataSet = context.getDataset(featureGenerationPipelineName);
		Scanner allFeatures = featureStatsDataSet.scan(null, null);
		Row row;
		SelectedFeatureStats featureStatsObj = new SelectedFeatureStats();

		while ((row = allFeatures.next()) != null) {
			FeatureStats featureStat = new FeatureStats();
			String featureName = row.getString(FeatureSTATS.Feature.getName());
			featureStat.setFeatureName(featureName);
			for (FeatureSTATS stat : FeatureSTATS.values()) {
				if(stat.equals(FeatureSTATS.Feature))
					continue;
				Object value = getStatColumnValue(stat, row);
				featureStat.addFeatureStat(stat.getName(), value);
			}
			featureStatsObj.addFeatureStat(featureStat);
		}
		responder.sendJson(featureStatsObj);
	}

	private Object getStatColumnValue(FeatureSTATS stat, Row row) {
		switch (stat.getType()) {
		case "long":
			return row.getLong(stat.getName());
		case "double":
			return row.getDouble(stat.getName());
		case "string":
			return row.getString(stat.getName());
		case "boolean":
			return row.getBoolean(stat.getName());
		}
		return null;
	}
}
