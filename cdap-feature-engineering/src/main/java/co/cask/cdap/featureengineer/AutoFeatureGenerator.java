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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.featureengineer.featuretool.FeatureToolCodeGenerator;
import co.cask.cdap.featureengineer.pipeline.CDAPPipelineDynamicSchemaGenerator;
import co.cask.cdap.featureengineer.pipeline.pojo.CDAPPipelineInfo;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchema;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchemaField;
import co.cask.cdap.featureengineer.pipeline.pojo.SchemaField;
import co.cask.cdap.featureengineer.pipeline.pojo.SchemaFieldName;
import co.cask.cdap.featureengineer.proto.FeatureGenerationRequest;
import co.cask.cdap.featureengineer.request.pojo.MultiFieldAggregationInput;
import co.cask.cdap.featureengineer.request.pojo.MultiSchemaColumn;
import co.cask.cdap.featureengineer.request.pojo.SchemaColumn;
import co.cask.cdap.featureengineer.utils.CommandExecutor;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.PluginSummary;
import co.cask.cdap.proto.artifact.preview.PreviewConfig;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;

/**
 * 
 * @author bhupesh.goel
 *
 */
public class AutoFeatureGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(AutoFeatureGenerator.class);

	static final Gson gsonObj = new GsonBuilder().setPrettyPrinting().create();

	private final FeatureGenerationRequest featureGenerationRequest;
	private final Map<String, NullableSchema> inputDataschemaMap;
	private final Map<String, CDAPPipelineInfo> wranglerPluginConfigMap;

	public static final class AutoFeatureGeneratorResult {
		private String featureEngineeringDAG;
		private String pipelineName;

		public AutoFeatureGeneratorResult(final String featureEngineeringDAG, final String pipelineName) {
			this.featureEngineeringDAG = featureEngineeringDAG;
			this.pipelineName = pipelineName;
		}

		public String getFeatureEngineeringDAG() {
			return featureEngineeringDAG;
		}

		public void setFeatureEngineeringDAG(String featureEngineeringDAG) {
			this.featureEngineeringDAG = featureEngineeringDAG;
		}

		public String getPipelineName() {
			return pipelineName;
		}

		public void setPipelineName(String pipelineName) {
			this.pipelineName = pipelineName;
		}

	}

	public AutoFeatureGenerator(FeatureGenerationRequest featureGenerationRequest,
			Map<String, NullableSchema> inputDataschemaMap, Map<String, CDAPPipelineInfo> wranglerPluginConfigMap) {
		this.featureGenerationRequest = featureGenerationRequest;
		this.inputDataschemaMap = inputDataschemaMap;
		this.wranglerPluginConfigMap = wranglerPluginConfigMap;
	}

	public AutoFeatureGeneratorResult generateFeatures(String[] hostAndPort) throws Exception {
		ClientConfig clientConfig = ClientConfig.builder()
				.setConnectionConfig(new ConnectionConfig(hostAndPort[0], Integer.parseInt(hostAndPort[1]), false))
				.build();
		Map<String, PluginSummary> aggregatePluginFunctionMap = new HashMap<String, PluginSummary>();
		Map<String, PluginSummary> transformPluginFunctionMap = new HashMap<String, PluginSummary>();

		Map<String, PluginSummary> multiInputAggregatePluginFunctionMap = new HashMap<String, PluginSummary>();
		Map<String, PluginSummary> multiInputTransformPluginFunctionMap = new HashMap<String, PluginSummary>();

		Set<String> typeSet = new HashSet<String>();
		List<String> entityNames = new LinkedList<String>();
		List<NullableSchema> dataSchemaList = new LinkedList<NullableSchema>();

		parseDataSchemaInJson(typeSet, entityNames, dataSchemaList);
		List<SchemaColumn> timestampColumns = featureGenerationRequest.getTimestampColumns();

		List<PluginSummary> pluginSummariesBatchAggregator = new LinkedList<PluginSummary>();
		List<PluginSummary> pluginSummariesTransform = new LinkedList<PluginSummary>();
		getCDAPPluginSummary(pluginSummariesBatchAggregator, pluginSummariesTransform, clientConfig);

		List<String> aggregatePrimitives = getPrimitives(pluginSummariesBatchAggregator, typeSet,
				aggregatePluginFunctionMap, featureGenerationRequest.getCategoricalColumns(),
				multiInputAggregatePluginFunctionMap, true);
		List<String> transformPrimitives = getPrimitives(pluginSummariesTransform, typeSet, transformPluginFunctionMap,
				null, multiInputTransformPluginFunctionMap, true);

		Map<String, Map<String, List<String>>> appliedTransFunctionsWithArguments = findMatchingMultiInputPrimitives(
				featureGenerationRequest.getMultiFieldTransformationFunctionInputs(),
				multiInputTransformPluginFunctionMap);
		Map<String, Map<String, List<String>>> appliedAggFunctionsWithArguments = findMatchingMultiInputPrimitives(
				featureGenerationRequest.getMultiFieldAggregationFunctionInputs(),
				multiInputAggregatePluginFunctionMap);

		String pythonScript = new FeatureToolCodeGenerator().generateFeatureToolCode(entityNames, dataSchemaList,
				featureGenerationRequest.getIndexes(), featureGenerationRequest.getRelationShips(), aggregatePrimitives,
				transformPrimitives, featureGenerationRequest.getDfsDepth(), featureGenerationRequest.getTargetEntity(),
				timestampColumns, featureGenerationRequest.getCategoricalColumns(),
				featureGenerationRequest.getIgnoreColumns(), appliedTransFunctionsWithArguments,
				appliedAggFunctionsWithArguments, transformPluginFunctionMap, aggregatePluginFunctionMap);

		LOG.debug("Generated Python Script = " + pythonScript);
		File pythonScriptFile = writeDataToTempFile(pythonScript, "python");

		String featureEngineeringDag = executeFeatureToolsAndGetFeatureDag(pythonScriptFile);
		LOG.info("Generated Feature Engineering DAG = " + featureEngineeringDag);
		CDAPPipelineDynamicSchemaGenerator pipelineGenerator = new CDAPPipelineDynamicSchemaGenerator(
				aggregatePluginFunctionMap, transformPluginFunctionMap, entityNames,
				featureGenerationRequest.getTargetEntity(), featureGenerationRequest.getTargetEntityFieldId(),
				featureGenerationRequest.getCategoricalColumns(), featureGenerationRequest.getWindowEndTime(),
				featureGenerationRequest.getTrainingWindows(), featureGenerationRequest.getTimeIndexColumns(),
				multiInputAggregatePluginFunctionMap, multiInputTransformPluginFunctionMap,
				appliedAggFunctionsWithArguments, appliedTransFunctionsWithArguments,
				featureGenerationRequest.getIndexes(), featureGenerationRequest.getPipelineRunName(), featureGenerationRequest.getRelationShips(), featureGenerationRequest.getCreateEntities());
		CDAPPipelineInfo pipelineInfo = pipelineGenerator.generateCDAPPipeline(featureEngineeringDag,
				inputDataschemaMap, wranglerPluginConfigMap);
		File cdapPipelineFile = writeDataToTempFile(gsonObj.toJson(pipelineInfo), "cdap-pipeline");
		String cdapPipelineFileName = cdapPipelineFile.getAbsolutePath();
		try {
			uploadPipelineFileToCDAP(cdapPipelineFileName, clientConfig, pipelineInfo.getName());
			LOG.info("successfully uploaded cdap pipeline " + pipelineInfo.getName() + " to CDAP server. ");
			return new AutoFeatureGeneratorResult(featureEngineeringDag, pipelineInfo.name);
		} catch (Throwable ex) {
			if (ex.getMessage().contains("duplicate key")) {
				LOG.debug("successfully uploaded cdap pipeline " + pipelineInfo.getName()
						+ " to CDAP server with exeption message = " + ex.getMessage());
				return new AutoFeatureGeneratorResult(featureEngineeringDag, pipelineInfo.name);
			} else
				throw ex;
		}
	}

	public Map<String, Map<String, List<String>>> findMatchingMultiInputPrimitives(Object multiFieldColumnsCombinationObject,
			Map<String, PluginSummary> multiInputPluginFunctionMap) {
		List multiFieldColumnsCombination = (List) multiFieldColumnsCombinationObject;
		Map<String, Map<String, List<String>>> matchingFunctionWithArguments = new HashMap<>();
		Map<String, Map<String, String>> columnWiseTableSchemaType = getColumnWiseTableSchemaMap(inputDataschemaMap);
		if (multiFieldColumnsCombination == null || multiFieldColumnsCombination.isEmpty())
			return matchingFunctionWithArguments;
		for (PluginSummary summary : multiInputPluginFunctionMap.values()) {
			for (int i = 0; i < summary.getPluginFunction().length; i++) {
				String inputType = summary.getPluginInput()[i];
				String[] inputArguments = inputType.split("\\s+");
				for (Object columnCombination : multiFieldColumnsCombination) {
					String destinationTable = null;
					String groupByKey = null;
					String destinationColumn = "";
					SchemaColumn[] columns = null;

					if (columnCombination instanceof MultiSchemaColumn) {
						columns = ((MultiSchemaColumn) columnCombination).getColumns().toArray(new SchemaColumn[0]);
					} else if (columnCombination instanceof MultiFieldAggregationInput) {
						MultiFieldAggregationInput aggInput = (MultiFieldAggregationInput) columnCombination;
						destinationColumn = aggInput.getDestinationColumn().getColumn();
						destinationTable = aggInput.getDestinationColumn().getTable();
						groupByKey = aggInput.getGroupByColumn().getColumn();
						columns = aggInput.getSourceColumns().toArray(new SchemaColumn[0]);
					}

					if (columns.length != inputArguments.length)
						continue;
					String tableName = columns[0].getTable();
					Map<String, String> columnTypeMap = columnWiseTableSchemaType.get(tableName);
					int matchedArgs = 0;
					StringBuilder primitiveArguments = new StringBuilder();
					StringBuilder argumentList = new StringBuilder();
					int count = 0;
					int matchedIndex = -1;
					for (int j = 0; j < columns.length; j++) {
						String columnName = columns[j].getColumn();
						String columnType = columnTypeMap.get(columnName);
						int matchingIndex = containsType(columnType, inputArguments[j].split(":"));
						if (matchingIndex >= 0) {
							if (matchedIndex == -1)
								matchedIndex = matchingIndex;
							if (matchedIndex == matchingIndex)
								matchedArgs++;
							if (count > 0) {
								primitiveArguments.append("_");
								argumentList.append(" ");
							}
							primitiveArguments.append(columnName.toLowerCase());
							argumentList.append(columnName.toLowerCase());
							count++;
						}
					}
					if (matchedArgs == columns.length) {
						Map<String, List<String>> columnMap = matchingFunctionWithArguments.get(tableName);
						if (columnMap == null) {
							columnMap = new HashMap<>();
							matchingFunctionWithArguments.put(tableName, columnMap);
						}

						columnMap.put(summary.getPluginFunction()[i] + "_" + primitiveArguments.toString(),
								Lists.newArrayList(summary.getPluginFunction()[i], argumentList.toString().trim(),
										getOutput(summary.getPluginOutput()[i], matchedIndex), groupByKey,
										destinationTable, destinationColumn));
						multiInputPluginFunctionMap.put(summary.getPluginFunction()[i], summary);
					}
				}
			}
		}
		return matchingFunctionWithArguments;
	}

	String getOutput(final String outputType, int matchedIndex) {
		if (outputType.startsWith("list"))
			return getOutputTypeFromListType(outputType);
		String[] tokens = outputType.split(":");
		if (tokens != null && tokens.length > matchedIndex)
			return tokens[matchedIndex];
		return outputType;
	}

	String getOutputTypeFromListType(String outputType) {
		int indexSt = outputType.indexOf('<');
		int indexEnd = outputType.indexOf('>');
		return outputType.substring(indexSt + 1, indexEnd);
	}

	int containsType(String columnType, String[] columnTypes) {
		int index = 0;
		for (String type : columnTypes) {
			if (type.equalsIgnoreCase(columnType) || (type.equals("datetime") && columnType.equals("string")))
				return index;
			index++;
		}
		return -1;
	}

	Map<String, Map<String, String>> getColumnWiseTableSchemaMap(final Map<String, NullableSchema> tableSchemaMap) {
		Map<String, Map<String, String>> columnWiseTableSchemaType = new HashMap<>();
		for (Map.Entry<String, NullableSchema> entry : tableSchemaMap.entrySet()) {
			Map<String, String> columnType = columnWiseTableSchemaType.get(entry.getKey());
			if (columnType == null) {
				columnType = new HashMap<>();
				columnWiseTableSchemaType.put(entry.getKey(), columnType);
			}
			for (SchemaFieldName fieldName : entry.getValue().getFields()) {
				columnType.put(fieldName.getName(), getSchemaType(fieldName));
			}
		}
		return columnWiseTableSchemaType;
	}

	public void uploadPipelineFileToCDAP(String cdapPipelineFileName, ClientConfig clientConfig, String pipelineName)
			throws UnauthenticatedException, IOException {
		ApplicationClient applicationClient = new ApplicationClient(clientConfig);
		ApplicationId appId = new ApplicationId("default", pipelineName);
		if (applicationClient.exists(appId)) {
			throw new IllegalArgumentException(
					"Provided Pipeline name " + pipelineName + " already exists. Please provide different filename");
		}
		Gson GSON = new Gson();
		JsonObject config = new JsonObject();
		String ownerPrincipal = null;
		Boolean updateSchedules = null;
		PreviewConfig previewConfig = null;

		if (cdapPipelineFileName != null) {
			File configFile = new File(cdapPipelineFileName);
			try (FileReader reader = new FileReader(configFile)) {
				AppRequest<JsonObject> appRequest = GSON.fromJson(reader, new TypeToken<AppRequest<JsonObject>>() {
				}.getType());
				config = appRequest.getConfig();
				ownerPrincipal = appRequest.getOwnerPrincipal();
				previewConfig = appRequest.getPreview();
				updateSchedules = appRequest.canUpdateSchedules();
			}
		}
		ArtifactSummary artifact = new ArtifactSummary("cdap-data-pipeline", "5.0.0", ArtifactScope.SYSTEM);
		AppRequest<JsonObject> appRequest = new AppRequest<>(artifact, config, previewConfig, ownerPrincipal,
				updateSchedules);

		applicationClient.deploy(appId, appRequest);
	}

	void writeToFile(String fileContent, String fileName) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
		bw.write(fileContent);
		bw.close();
	}

	String executeFeatureToolsAndGetFeatureDag(File pythonScriptFile) throws IOException, InterruptedException {
		CommandExecutor executor = new CommandExecutor();
		executor.executeCommand("python", pythonScriptFile.getAbsolutePath());
		return executor.getCommandOutput();
	}

	public void parseDataSchemaInJson(Set<String> typeSet, List<String> entityNames, List<NullableSchema> dataSchemaList) {

		for (Map.Entry<String, NullableSchema> entry : inputDataschemaMap.entrySet()) {
			NullableSchema dataSchema = entry.getValue();
			dataSchemaList.add(dataSchema);
			populateTypeSet(dataSchema, typeSet);
			entityNames.add(entry.getKey());
		}
	}

	File writeDataToTempFile(String data, String key) throws IOException {
		File tmpFile = File.createTempFile("temp-feature-engineering-key", ".tmp");
		FileWriter writer = new FileWriter(tmpFile);
		writer.write(data);
		writer.close();
		return tmpFile;
	}

	public List<String> getPrimitives(List<PluginSummary> pluginSummaries, Set<String> typeSet,
			Map<String, PluginSummary> pluginFunctionMap, List<SchemaColumn> CategoricalColumns,
			Map<String, PluginSummary> multiInputPluginFunctionMap, boolean getDynamicPrimitives) {
		Set<String> primitives = new HashSet<String>();
		if (pluginSummaries == null)
			return new LinkedList<String>();
		outer: while (true) {
			for (PluginSummary summary : pluginSummaries) {
				if (summary == null || summary.getPluginFunction() == null || summary.getPluginFunction().length == 0)
					continue;
				for (int i = 0; i < summary.getPluginFunction().length; i++) {
					String inputType = summary.getPluginInput()[i];
					String outputType = summary.getPluginOutput()[i];
					if (inputType.contains(" ")) {
						multiInputPluginFunctionMap.put(summary.getName(), summary);
						continue;
					}
					if (outputType.startsWith("list") && CategoricalColumns == null)
						continue;
					String[] outputTypeToken = outputType.split(":");
					boolean finalResult = false;
					for (String token : outputTypeToken) {
						boolean result = typeSet.add(token.toLowerCase());
						if (result)
							finalResult = true;
					}
					if (finalResult)
						continue outer;
					if (inputType.equals("*")) {
						primitives.add(summary.getPluginFunction()[i]);
						pluginFunctionMap.put(summary.getPluginFunction()[i].toLowerCase(), summary);
					} else {
						String[] inputTypeToken = inputType.split(":");
						inputTypeToken = addInferiorDataTypes(inputTypeToken);
						for (String token : inputTypeToken) {
							if (typeSet.contains(token.toLowerCase()) || (token.equals("datetime") && typeSet.contains("string"))) {
								primitives.add(summary.getPluginFunction()[i]);
								pluginFunctionMap.put(summary.getPluginFunction()[i].toLowerCase(), summary);
								break;
							}
						}
					}

				}
			}
			return new LinkedList<String>(primitives);
		}

	}

	String[] addInferiorDataTypes(String[] inputTypeToken) {
		Set<String> inputTypeTokenSet = new HashSet<String>();
		for (String token : inputTypeToken) {
			if (token.equalsIgnoreCase("long")) {
				inputTypeTokenSet.add("int");
			} else if (token.equalsIgnoreCase("double")) {
				inputTypeTokenSet.add("int");
				inputTypeTokenSet.add("long");
				inputTypeTokenSet.add("float");
			}
			inputTypeTokenSet.add(token);
		}
		return new LinkedList<String>(inputTypeTokenSet).toArray(new String[0]);
	}

	public void getCDAPPluginSummary(List<PluginSummary> pluginSummariesBatchAggregator,
			List<PluginSummary> pluginSummariesTransform, ClientConfig clientConfig)
			throws UnauthenticatedException, ArtifactNotFoundException, IOException, UnauthorizedException {
		// Interact with the CDAP instance located at example.com, port 11015

		// Construct the client used to interact with CDAP
		ArtifactClient artifactClient = new ArtifactClient(clientConfig);
		ArtifactId artifactId = new ArtifactId("default", "cdap-data-pipeline", "5.0.0");

		List<PluginSummary> pluginSummariesBatchAggregatorTmp = artifactClient.getPluginSummaries(artifactId,
				"batchaggregator", ArtifactScope.SYSTEM);
		List<PluginSummary> pluginSummariesTransformTmp = artifactClient.getPluginSummaries(artifactId, "transform",
				ArtifactScope.SYSTEM);
		pluginSummariesBatchAggregator.addAll(pluginSummariesBatchAggregatorTmp);
		pluginSummariesTransform.addAll(pluginSummariesTransformTmp);
	}

	void populateTypeSet(final NullableSchema dataSchema, final Set<String> typeSet) {
		for (SchemaFieldName field : dataSchema.getFields()) {
			typeSet.add(getSchemaType(field).toLowerCase());
		}
	}

	String getSchemaType(SchemaFieldName field) {
		if (field instanceof SchemaField) {
			return ((SchemaField) field).getType();
		} else if (field instanceof NullableSchemaField) {
			return ((NullableSchemaField) field).getType().get(0);
		}
		return null;
	}
}
