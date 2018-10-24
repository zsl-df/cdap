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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.POST;
import javax.ws.rs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.artifact.ArtifactManager;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.featureengineer.AutoFeatureGenerator;
import co.cask.cdap.featureengineer.AutoFeatureGenerator.AutoFeatureGeneratorResult;
import co.cask.cdap.featureengineer.FeatureEngineeringApp.FeatureEngineeringConfig;
import co.cask.cdap.featureengineer.RequestExtractor;
import co.cask.cdap.featureengineer.pipeline.pojo.CDAPPipelineInfo;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchema;
import co.cask.cdap.featureengineer.proto.FeatureGenerationRequest;
import co.cask.cdap.featureengineer.utils.JSONInputParser;

/**
 * @author bhupesh.goel
 *
 */
public class AutoFeatureGenerationServiceHandler extends BaseServiceHandler {

	private static final Logger LOG = LoggerFactory.getLogger(AutoFeatureGenerationServiceHandler.class);

	@Property
	private final String dataSchemaTableName;
	@Property
	private final String pluginConfigTableName;
	@Property
	private final String featureDAGTableName;
	@Property
	private final String featureEngineeringConfigTableName;

	private KeyValueTable dataSchemaTable;
	private KeyValueTable pluginConfigTable;
	private KeyValueTable featureDAGTable;
	private KeyValueTable featureEngineeringConfigTable;

	/**
	 * @param config
	 * 
	 */
	public AutoFeatureGenerationServiceHandler(FeatureEngineeringConfig config) {
		this.dataSchemaTableName = config.getDataSchemaTable();
		this.pluginConfigTableName = config.getPluginConfigTable();
		this.featureDAGTableName = config.getFeatureDAGTable();
		this.featureEngineeringConfigTableName = config.getFeatureEngineeringConfigTable();
	}

	@Override
	public void initialize(HttpServiceContext context) throws Exception {
		super.initialize(context);
		this.dataSchemaTable = context.getDataset(dataSchemaTableName);
		this.pluginConfigTable = context.getDataset(pluginConfigTableName);
		this.featureDAGTable = context.getDataset(featureDAGTableName);
		this.featureEngineeringConfigTable = context.getDataset(featureEngineeringConfigTableName);
	}

	@POST
	@Path("featureengineering/features/create")
	public void generateFeatures(HttpServiceRequest request, HttpServiceResponder responder) {
		Map<String, NullableSchema> inputDataschemaMap = null;
		try {
			FeatureGenerationRequest featureGenerationRequest = new RequestExtractor(request).getContent("UTF-8",
					FeatureGenerationRequest.class);
			inputDataschemaMap = getSchemaMap(featureGenerationRequest.getDataSchemaNames());
			Map<String, CDAPPipelineInfo> wranglerPluginConfigMap = getWranglerPluginConfigMap(
					featureGenerationRequest.getDataSchemaNames());
			String hostAndPort[] = getHostAndPort(request);
			AutoFeatureGeneratorResult result = new AutoFeatureGenerator(featureGenerationRequest, inputDataschemaMap,
					wranglerPluginConfigMap).generateFeatures(hostAndPort);
			for (String dataSchema : inputDataschemaMap.keySet()) {
				dataSchemaTable.write("DataSchema_" + dataSchema + "_" + result.getPipelineName(),
						JSONInputParser.convertToJSON(inputDataschemaMap.get(dataSchema)));
				pluginConfigTable.write("PluginConfig_" + dataSchema + "_" + result.getPipelineName(),
						JSONInputParser.convertToJSON(wranglerPluginConfigMap.get(dataSchema)));
			}
			featureDAGTable.write(result.getPipelineName(), result.getFeatureEngineeringDAG());
			featureEngineeringConfigTable.write(result.getPipelineName(),
					JSONInputParser.convertToJSON(featureGenerationRequest));
			success(responder, "Successfully Generated Features for data schemas " + inputDataschemaMap.keySet()
					+ " with pipeline name = " + result.getPipelineName());
		} catch (Exception e) {
			error(responder, "Failed to generate features for data schemas " + inputDataschemaMap.keySet()
					+ " with error message " + e.getMessage());
		}
	}

	private String[] getHostAndPort(HttpServiceRequest request) {
		return request.getHeader("Host").split(":");
	}

	private Map<String, CDAPPipelineInfo> getWranglerPluginConfigMap(List<String> dataSchemaNames) {
		Map<String, CDAPPipelineInfo> wranglerPluginConfigMap = new HashMap<String, CDAPPipelineInfo>();
		for (String schemaName : dataSchemaNames) {
			byte[] pluginConfigBytes = pluginConfigTable.read(schemaName + "_batch");
			String pluginConfig = new String(pluginConfigBytes, StandardCharsets.UTF_8);
			wranglerPluginConfigMap.put(schemaName, JSONInputParser.parseWranglerPlugin(pluginConfig));
		}
		return wranglerPluginConfigMap;
	}

	private Map<String, NullableSchema> getSchemaMap(final List<String> dataSchemaNames) {
		Map<String, NullableSchema> schemaMap = new HashMap<String, NullableSchema>();
		for (String schemaName : dataSchemaNames) {
			byte[] schemaBytes = dataSchemaTable.read(schemaName);
			String schema = new String(schemaBytes, StandardCharsets.UTF_8);
			schemaMap.put(schemaName, JSONInputParser.parseDataSchemaJSON(schema));
		}
		return schemaMap;
	}

}
