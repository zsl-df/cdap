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
package co.cask.cdap.featureengineer.pipeline;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import co.cask.cdap.common.enums.FeatureSTATS;
import co.cask.cdap.featureengineer.pipeline.pojo.*;
import co.cask.cdap.featureengineer.request.pojo.SchemaColumn;
import co.cask.cdap.proto.artifact.PluginSummary;

/**
 * @author bhupesh.goel
 *
 */
public class CDAPPipelineDynamicSchemaGenerator {

	private static DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();

	private static final Gson gsonObj = new GsonBuilder().setPrettyPrinting().create();
	private final Map<String, BasePipelineNode> stageMap;
	private final Map<String, Schema> schemaMap;
	private final Map<String, Set<String>> connections;
	private final Set<String> isNodeReachable;

	private final Artifact systemArtifact;
	private final Artifact pluginArtifact;
	private final Artifact featureEngineeringArtifact;
	private final Set<String> specificPluginNames;
	private final String pipelineName;

	Map<String, PluginSummary> aggregatePluginFunctionMap;
	Map<String, PluginSummary> transformPluginFunctionMap;
	Map<String, String> lastStageMapForTable;
	List<String> entityNames;
	Map<String, Map<String, Map<String, String>>> functionDataTypeInfoMapTransform;
	Map<String, Map<String, Map<String, String>>> functionDataTypeInfoMapAggregate;
	Set<String> isUsedStage;
	String targetEntity;
	String targetEntityIdField;
	Map<String, Set<String>> categoricalColumnMap;
	List<SchemaColumn> categoricalColumns;
	DateTime windowEndTime;
	List<Integer> trainingWindows;
	List<SchemaColumn> timeIndexColumns;
	Map<String, Set<String>> categoricalColumnsToBeChecked;

	Map<String, PluginSummary> multiInputAggregatePluginFunctionMap;
	Map<String, PluginSummary> multiInputTransformPluginFunctionMap;
	Map<String, Map<String, List<String>>> appliedAggFunctionsWithArguments;
	Map<String, Map<String, List<String>>> appliedTransFunctionsWithArguments;
	Map<String, String> tableIndexMap;

	private int globalUniqueID;
	private static final String STAGE_NAME = "stage";
	private final Map<String, String> generatedStageMap;
	private final Map<String, String> generatedReverseStageMap;

	private static final String NUM_PARTITIONS = "5";
	private static final boolean enablePartitions = false;

	/**
	 * @param transformPluginFunctionMap
	 * @param aggregatePluginFunctionMap
	 * @param targetEntity
	 * @param targetEntityIdField
	 * @param trainingWindows
	 * @param windowEndTime
	 * @param categoricalColumnDictionary
	 * @param timeIndexColumns
	 * @param appliedTransFunctionsWithArguments
	 * @param appliedAggFunctionsWithArguments
	 * @param multiInputTransformPluginFunctionMap
	 * @param multiInputAggregatePluginFunctionMap
	 * @param indexes
	 * @param pipelineName
	 * 
	 */
	public CDAPPipelineDynamicSchemaGenerator(Map<String, PluginSummary> aggregatePluginFunctionMap,
			Map<String, PluginSummary> transformPluginFunctionMap, List<String> entityNames, String targetEntity,
			String targetEntityIdField, List<SchemaColumn> categoricalColumns, String windowEndTime,
			List<Integer> trainingWindows, List<SchemaColumn> timeIndexColumns,
			Map<String, PluginSummary> multiInputAggregatePluginFunctionMap,
			Map<String, PluginSummary> multiInputTransformPluginFunctionMap,
			Map<String, Map<String, List<String>>> appliedAggFunctionsWithArguments,
			Map<String, Map<String, List<String>>> appliedTransFunctionsWithArguments, List<SchemaColumn> indexes,
			String pipelineName) {
		this.stageMap = new LinkedHashMap<String, BasePipelineNode>();
		this.connections = new LinkedHashMap<String, Set<String>>();
		this.systemArtifact = new Artifact();
		this.pluginArtifact = new Artifact();
		this.featureEngineeringArtifact = new Artifact();
		this.isNodeReachable = new HashSet<String>();
		this.aggregatePluginFunctionMap = aggregatePluginFunctionMap;
		this.transformPluginFunctionMap = transformPluginFunctionMap;
		this.lastStageMapForTable = new HashMap<String, String>();
		this.entityNames = entityNames;
		for (String entity : entityNames) {
			lastStageMapForTable.put(entity, entity);
		}
		this.pipelineName = pipelineName;
		globalUniqueID = 0;
		this.generatedStageMap = new HashMap<String, String>();
		this.generatedReverseStageMap = new HashMap<>();
		this.categoricalColumnsToBeChecked = new HashMap<String, Set<String>>();
		this.timeIndexColumns = timeIndexColumns;
		functionDataTypeInfoMapTransform = new HashMap<String, Map<String, Map<String, String>>>();
		functionDataTypeInfoMapAggregate = new HashMap<String, Map<String, Map<String, String>>>();
		this.tableIndexMap = new HashMap<>();
		for (SchemaColumn index : indexes) {
			this.tableIndexMap.put(index.getTable(), index.getColumn());
		}
		this.multiInputAggregatePluginFunctionMap = multiInputAggregatePluginFunctionMap;
		this.multiInputTransformPluginFunctionMap = multiInputTransformPluginFunctionMap;
		this.appliedAggFunctionsWithArguments = appliedAggFunctionsWithArguments;
		this.appliedTransFunctionsWithArguments = appliedTransFunctionsWithArguments;

		generateFunctionDataTypeInfoMap(functionDataTypeInfoMapAggregate, aggregatePluginFunctionMap);
		generateFunctionDataTypeInfoMap(functionDataTypeInfoMapTransform, transformPluginFunctionMap);

		this.schemaMap = new HashMap<String, Schema>();
		systemArtifact.setName("cdap-data-pipeline");
		systemArtifact.setScope("SYSTEM");
		systemArtifact.setVersion("5.0.0");
		pluginArtifact.setName("core-plugins");
		pluginArtifact.setScope("SYSTEM");
		pluginArtifact.setVersion("2.0.0");
		featureEngineeringArtifact.setName("feature-engineering-plugin");
		featureEngineeringArtifact.setScope("SYSTEM");
		featureEngineeringArtifact.setVersion("2.0.0");

		specificPluginNames = new HashSet<String>();
		specificPluginNames.add("name");
		specificPluginNames.add("type");
		specificPluginNames.add("schema");
		specificPluginNames.add("pluginName");
		isUsedStage = new HashSet<String>();
		this.targetEntity = targetEntity;
		this.targetEntityIdField = targetEntityIdField;
		this.categoricalColumns = new LinkedList<SchemaColumn>();
		generateCatetgoricalColumnMap(categoricalColumns);
		if (windowEndTime != null)
			this.windowEndTime = formatter.parseDateTime(windowEndTime);
		this.trainingWindows = trainingWindows;
	}

	private final String getNextUniqueIdForStage(final String stage) {
		globalUniqueID++;
		String uniqueStageId = STAGE_NAME + "_" + globalUniqueID;
		this.generatedStageMap.put(uniqueStageId, stage);
		this.generatedReverseStageMap.put(stage, uniqueStageId);
		return uniqueStageId;
	}

	private Map<String, Set<String>> generateCatetgoricalColumnMap(List<SchemaColumn> categoricalColumns) {
		this.categoricalColumnMap = new HashMap<String, Set<String>>();
		if (categoricalColumns != null)
			for (SchemaColumn column : categoricalColumns) {
				this.categoricalColumns.add(column);
				Set<String> columnSet = categoricalColumnMap.get(column.getTable().toLowerCase());
				if (columnSet == null) {
					columnSet = new HashSet<String>();
					categoricalColumnMap.put(column.getTable().toLowerCase(), columnSet);
				}
				columnSet.add(column.getColumn().toLowerCase());
			}
		return this.categoricalColumnMap;
	}

	private void putInConnection(String source, String dest) {
		Set<String> destSet = connections.get(source);
		if (destSet == null) {
			destSet = new LinkedHashSet<>();
			connections.put(source, destSet);
		}
		destSet.add(dest);
	}

	private void generateFunctionDataTypeInfoMap(Map<String, Map<String, Map<String, String>>> functionDataTypeInfoMap,
			Map<String, PluginSummary> pluginSummary) {
		for (PluginSummary summary : pluginSummary.values()) {
			Map<String, Map<String, String>> functionDataTypeMap = functionDataTypeInfoMap.get(summary.getName());
			if (functionDataTypeMap == null) {
				functionDataTypeMap = new HashMap<String, Map<String, String>>();
				functionDataTypeInfoMap.put(summary.getName(), functionDataTypeMap);
			}
			for (int i = 0; i < summary.getPluginFunction().length; i++) {
				String function = summary.getPluginFunction()[i].toLowerCase();
				String inputTypes = summary.getPluginInput()[i];
				String outputTypes = summary.getPluginOutput()[i];
				Map<String, String> inputOutputMap = functionDataTypeMap.get(function);
				if (inputOutputMap == null) {
					inputOutputMap = new HashMap<String, String>();
					functionDataTypeMap.put(function, inputOutputMap);
				}
				String[] tokens = outputTypes.split(":");
				if (tokens.length == 1) {
					String[] tokens2 = inputTypes.split(":");
					for (String input : tokens2) {
						inputOutputMap.put(input, tokens[0]);
					}
				} else {
					tokens = outputTypes.split(":");
					String[] tokens2 = inputTypes.split(":");
					if (tokens.length != tokens2.length) {
						throw new IllegalStateException("Function Input Output types are not matching");
					}
					for (int j = 0; j < tokens.length; j++) {
						inputOutputMap.put(tokens2[j], tokens[j]);
					}
				}
				if (inputOutputMap.containsKey("double")) {
					inputOutputMap.put("long", inputOutputMap.get("double"));
					inputOutputMap.put("int", inputOutputMap.get("double"));
					inputOutputMap.put("float", inputOutputMap.get("double"));
				}
			}

		}
	}

	public CDAPPipelineInfo generateCDAPPipeline(final String featureDag,
			Map<String, NullableSchema> inputDataSourceInfoMap, Map<String, CDAPPipelineInfo> wranglerPluginConfigMap) {
		CDAPPipelineInfo pipelineInformation = new CDAPPipelineInfo();
		if (StringUtils.isEmpty(pipelineName))
			pipelineInformation.setName("Pipeline_" + System.currentTimeMillis());
		else
			pipelineInformation.setName(pipelineName);
		pipelineInformation.setArtifact(systemArtifact);
		PipelineConfiguration config = generatePipelineConfiguration(featureDag, inputDataSourceInfoMap,
				wranglerPluginConfigMap);
		pipelineInformation.setConfig(config);
		System.out.println("generatedStageMap = \n");
		for (Map.Entry<String, String> entry : generatedStageMap.entrySet()) {
			System.out.println(entry.getKey() + " = " + entry.getValue());
		}
		return pipelineInformation;
	}

	private PipelineConfiguration generatePipelineConfiguration(String featureDag,
			Map<String, NullableSchema> inputDataSourceInfoMap, Map<String, CDAPPipelineInfo> wranglerPluginConfigMap) {
		PipelineConfiguration pipeLineConfiguration = new PipelineConfiguration();
		pipeLineConfiguration.setEngine("spark");
		// pipeLineConfiguration.setEngine("mapreduce");
		pipeLineConfiguration.setInstances(4);
		pipeLineConfiguration.setSchedule("9 15 2 7 *");
		pipeLineConfiguration.setPostActions(new LinkedList<BasePipelineNode>());
		Map<String, Integer> driverResources = new HashMap<String, Integer>();
		driverResources.put("memoryMB", 3072);
		driverResources.put("virtualCores", 1);
		pipeLineConfiguration.setDriverResources(driverResources);
		Map<String, Integer> resources = new HashMap<String, Integer>();
		resources.put("memoryMB", 10240);
		resources.put("virtualCores", 3);
		pipeLineConfiguration.setResources(resources);

		createSourceStages(inputDataSourceInfoMap, wranglerPluginConfigMap, pipeLineConfiguration);
		Map<String, String> lastStageMapForTableTillSource = new HashMap<>(lastStageMapForTable);
		List<String> lastStagesForEachTrainingWindow = new LinkedList<String>();
		List<String> statsStagesForEachTrainingWindow = new LinkedList<String>();
		if (this.trainingWindows != null && !this.trainingWindows.isEmpty()) {
			for (Integer trainingTime : this.trainingWindows) {
				categoricalColumnsToBeChecked.clear();
				DateTime windowStartTime = windowEndTime.minusHours(trainingTime);
				this.lastStageMapForTable = new HashMap<>(lastStageMapForTableTillSource);
				for (SchemaColumn timeIndexColumn : timeIndexColumns) {
					createFilterStage(windowStartTime, windowEndTime, timeIndexColumn);
				}
				populateStagesFromOperations(featureDag);
				lastStagesForEachTrainingWindow.add(lastStageMapForTable.get(targetEntity));
				//String statsComputeStageName = createStatsComputeStage(lastStageMapForTable.get(targetEntity));
				//statsStagesForEachTrainingWindow.add(statsComputeStageName);
			}
			 String sourceTempTableName = takeOuterjoinOfAllTempTables(lastStagesForEachTrainingWindow, targetEntityIdField);
			 String statsComputeStageName = createStatsComputeStage(sourceTempTableName);
//			String joinedStatsTableName = takeOuterjoinOfAllTempTables(statsStagesForEachTrainingWindow, "Statistic");
			// getElasticSearchSinkNode("StatsElasticsearch", "statsDataSink",
			// "stats_index_" + System.currentTimeMillis(),
			// "stats", "Statistic", joinedStatsTableName);
			getCDAPTableSinkNode("FeatureStats", statsComputeStageName);
			// getFileSinkNode("StatsFileSink", "statsDataSink", "stats_index_",
			// joinedStatsTableName);
			// lastStageMapForTable.put(targetEntity, sourceTempTableName);
			completePipelineAndSerializeIt(pipeLineConfiguration);

		} else
			generatePipelineStagesAndCreateConnections(pipeLineConfiguration, featureDag);
		return pipeLineConfiguration;
	}

	private String createStatsComputeStage(String lastStageName) {
		PipelineNode stageNode = new PipelineNode();
		String stageName = lastStageName + "_stats";
		stageNode.setName(stageName);
		PluginNode pluginNode = new PluginNode();
		stageNode.setPlugin(pluginNode);
		pluginNode.setLabel(stageName);
		pluginNode.setName("StatsComputeInMemory");
		pluginNode.setType("sparkcompute");
		Artifact esArtifact = new Artifact();
		esArtifact.setName("feature-engineering-plugin");
		esArtifact.setVersion("2.0.0");
		esArtifact.setScope("SYSTEM");
		pluginNode.setArtifact(esArtifact);
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("parallelThreads", "20");
		pluginNode.setProperties(properties);

		putInConnection(lastStageName, stageName);
		stageMap.put(stageName, stageNode);
		isUsedStage.add(lastStageName);
		isUsedStage.add(stageName);
		return stageName;
	}

	private Schema generateStatsComputeOutputSchema(Schema lastStageSchema) {
		Schema curSchema = new Schema();
		curSchema.setName("etlSchemaBody");
		curSchema.setType("record");
		List<SchemaFieldName> fields = new LinkedList<SchemaFieldName>();
		SchemaField firstField = new SchemaField();
		firstField.setName("Statistic");
		firstField.setType("string");
		fields.add(firstField);
		for (SchemaFieldName field : lastStageSchema.getFields()) {
			fields.add(getNullableSchema(field));
		}
		curSchema.setFields(fields.toArray(new SchemaFieldName[0]));
		return curSchema;
	}

	private String takeOuterjoinOfAllTempTables(List<String> currentStageTempTableNames, String sourceJoinKey) {
		if (currentStageTempTableNames.isEmpty())
			return null;
		String lastJoinerTempTableName = currentStageTempTableNames.get(0);
		for (int index = 1; index < currentStageTempTableNames.size(); index++) {
			String destTableLastStage = lastJoinerTempTableName;
			String sourceTableLastStage = currentStageTempTableNames.get(index);
			lastJoinerTempTableName = takeOuterJoinOfTwoTables(sourceTableLastStage, destTableLastStage, sourceJoinKey,
					sourceJoinKey, index);
		}
		return lastJoinerTempTableName;
	}

	private String takeOuterJoinOfTwoTables(String sourceTableLastStage, String destTableLastStage,
			String sourceJoinKey, String destJoinKey, int index) {
		String stageName = "";
		if (this.generatedStageMap.containsKey(destTableLastStage)) {
			stageName = this.generatedStageMap.get(destTableLastStage) + "_joiner_";
		} else
			stageName = destTableLastStage + "_joiner_";
		if (this.generatedStageMap.containsKey(sourceTableLastStage)) {
			stageName += this.generatedStageMap.get(sourceTableLastStage);
		} else
			stageName += sourceTableLastStage;

		stageName = getNextUniqueIdForStage(stageName);
		stageName = "Joiner_" + stageName;
		PipelineNode pipelineNode = new PipelineNode();
		pipelineNode.setName(stageName);
		PluginNode pluginNode = new PluginNode();
		pipelineNode.setPlugin(pluginNode);
		pluginNode.setArtifact(this.featureEngineeringArtifact);
		pluginNode.setName("JoinerDynamic");
		pluginNode.setType("batchjoiner");
		pluginNode.setLabel(stageName);
		Map<String, Object> pluginProperties = new HashMap<String, Object>();
		pluginNode.setProperties(pluginProperties);
		Map<String, String> keysToBeAppendedMap = new HashMap<String, String>();
		if (index == 1) {
			keysToBeAppendedMap.put(destTableLastStage, "_" + this.trainingWindows.get(0));
		}
		// else
		// keysToBeAppendedMap.put(destTableLastStage, "");
		keysToBeAppendedMap.put(sourceTableLastStage, "_" + this.trainingWindows.get(index));

		pluginProperties.put("joinKeys",
				sourceTableLastStage + "." + sourceJoinKey + " = " + destTableLastStage + "." + destJoinKey);
		pluginProperties.put("requiredInputs", sourceTableLastStage);
		pluginProperties.put("selectedFields", ",");
		pluginProperties.put("keysToBeAppended", generateKeysToBeAppended(keysToBeAppendedMap));

		stageMap.put(stageName, pipelineNode);
		putInConnection(sourceTableLastStage, stageName);
		putInConnection(destTableLastStage, stageName);
		isUsedStage.add(sourceTableLastStage);
		isUsedStage.add(stageName);
		isUsedStage.add(destTableLastStage);
		return stageName;
	}

	private String generateKeysToBeAppended(Map<String, String> keysToBeAppendedMap) {
		StringBuilder sb = new StringBuilder();
		int index = 0;
		for (Map.Entry<String, String> entry : keysToBeAppendedMap.entrySet()) {
			if (index > 0)
				sb.append(",");
			sb.append(entry.getKey());
			sb.append(".");
			sb.append(entry.getValue());
			index++;
		}
		return sb.toString();
	}

	private NullableSchemaField getNullableSchema(SchemaFieldName schemaField) {
		NullableSchemaField schema = new NullableSchemaField();
		schema.setName(schemaField.getName());
		List<String> list = new LinkedList<>();
		if (schemaField instanceof NullableSchemaField) {
			schema.setType(((NullableSchemaField) schemaField).getType());
		} else {
			list.add(((SchemaField) schemaField).getType());
			list.add("null");
			schema.setType(list);
		}
		return schema;
	}

	private void generatePipelineStagesAndCreateConnections(PipelineConfiguration pipeLineConfiguration,
			String featureDag) {
		populateStagesFromOperations(featureDag);
		completePipelineAndSerializeIt(pipeLineConfiguration);
	}

	private void completePipelineAndSerializeIt(PipelineConfiguration pipeLineConfiguration) {
		// getElasticSearchSinkNode("Elasticsearch", "dataSink",
		// targetEntity.toLowerCase() + "_index_" + System.currentTimeMillis(),
		// targetEntity.toLowerCase(),
		// targetEntityIdField, lastStageMapForTable.get(targetEntity));
		// getFileSinkNode("FileSink", "dataSink", targetEntity.toLowerCase() +
		// "_index_", lastStageMapForTable.get(targetEntity));
		markReachableNodesFromTargetEntity(targetEntity);
		truncateIslandNodesFromDAG();
		generateTrashSinkStage();
		List<Connection> connectionList = new LinkedList<Connection>();
		for (Map.Entry<String, Set<String>> entry : connections.entrySet()) {
			String source = entry.getKey();
			for (String dest : entry.getValue()) {
				Connection connection = new Connection();
				connection.setFrom(source);
				connection.setTo(dest);
				connectionList.add(connection);
			}
		}
		pipeLineConfiguration.setConnections(connectionList);
		stageMap.keySet().retainAll(isUsedStage);

		pipeLineConfiguration.setStages(new LinkedList<BasePipelineNode>(stageMap.values()));
	}

	private void populateStagesFromOperations(String featureDag) {
		computeManualyProvidedVariables();
		String featureList[] = featureDag.split("\n");
		for (int i = 0; i < featureList.length; i++) {
			String feature = featureList[i].trim();
			try {
				Integer.parseInt(feature);
				continue;
			} catch (Exception e) {
				String sourceTables = feature.trim();
				String operations = featureList[i + 1].trim();
				populateStagesFromOperations(sourceTables, operations);
				i++;
			}
		}
	}

	private void computeManualyProvidedVariables() {
		computeManualyProvidedTransVariables();
		computeManualyProvidedAggVariables();
	}

	private void computeManualyProvidedTransVariables() {
		for (Map.Entry<String, Map<String, List<String>>> entry : this.appliedTransFunctionsWithArguments.entrySet()) {
			String tableName = entry.getKey();
			Map<String, List<String>> columnDataTypeMap = entry.getValue();

			Map<String, Map<String, List<String>>> pluginColumnDataTypeMap = new HashMap<>();
			Map<String, PluginSummary> pluginSummaryMap = new HashMap<>();
			getPluginColumnDataTyepMap(columnDataTypeMap, pluginColumnDataTypeMap, pluginSummaryMap,
					this.multiInputTransformPluginFunctionMap);

			for (Map.Entry<String, Map<String, List<String>>> entry2 : pluginColumnDataTypeMap.entrySet()) {
				String lastStageName = this.lastStageMapForTable.get(tableName);
				PluginSummary pluginSummary = pluginSummaryMap.get(entry2.getKey());
				String currentStageName = lastStageName + "_" + entry2.getKey() + "_" + System.currentTimeMillis();
				currentStageName = getNextUniqueIdForStage(currentStageName);
				currentStageName = entry2.getKey() + "_" + currentStageName;
				PipelineNode pipelineNode = new PipelineNode();
				PluginNode pluginNode = new PluginNode();
				pipelineNode.setPlugin(pluginNode);
				pipelineNode.setName(currentStageName);
				pluginNode.setName(pluginSummary.getName());
				pluginNode.setType(pluginSummary.getType());
				pluginNode.setLabel(currentStageName);
				pluginNode.setArtifact(this.featureEngineeringArtifact);
				Map<String, Object> properties = new HashMap<String, Object>();
				StringBuilder sb = new StringBuilder();

				int index = 0;
				for (Map.Entry<String, List<String>> entry3 : entry2.getValue().entrySet()) {
					if (index > 0)
						sb.append(",");
					sb.append(entry3.getKey() + ":" + entry3.getValue().get(0).trim().toUpperCase() + "(");
					sb.append(entry3.getValue().get(1).trim());
					sb.append(")");
					index++;
				}
				properties.put("primitives", sb.toString());
				properties.put("isDynamicSchema", true);

				pluginNode.setProperties(properties);

				putInConnection(lastStageMapForTable.get(tableName), currentStageName);
				isUsedStage.add(lastStageMapForTable.get(tableName));
				isUsedStage.add(currentStageName);
				stageMap.put(currentStageName, pipelineNode);
				lastStageMapForTable.put(tableName, currentStageName);
			}

		}

	}

	private void getPluginColumnDataTyepMap(Map<String, List<String>> columnDataTypeMap,
			Map<String, Map<String, List<String>>> pluginColumnDataTypeMap, Map<String, PluginSummary> pluginSummaryMap,
			Map<String, PluginSummary> multiInputTransformPluginFunctionMap2) {
		for (Map.Entry<String, List<String>> entry : columnDataTypeMap.entrySet()) {
			String columnName = entry.getKey().trim();
			// String[] token = columnName.substring(0, columnName.length() - 1).split("_");
			PluginSummary summary = multiInputTransformPluginFunctionMap2.get(entry.getValue().get(0));
			pluginSummaryMap.put(summary.getName(), summary);
			Map<String, List<String>> columnDataTypeMap2 = pluginColumnDataTypeMap.get(summary.getName());
			if (columnDataTypeMap2 == null) {
				columnDataTypeMap2 = new HashMap<>();
				pluginColumnDataTypeMap.put(summary.getName(), columnDataTypeMap2);
			}
			columnDataTypeMap2.put(entry.getKey(), entry.getValue());
		}
	}

	private void createFilterStage(DateTime windowStartTime, DateTime windowEndTime, SchemaColumn timeIndexColumn) {
		String tableName = timeIndexColumn.getTable().toLowerCase();
		String columnName = timeIndexColumn.getColumn().toLowerCase();
		String lastStageName = this.lastStageMapForTable.get(tableName);
		String currentStageName = lastStageName + "_filter_" + windowStartTime.getMillis();
		currentStageName = getNextUniqueIdForStage(currentStageName);
		currentStageName = "Filter_" + currentStageName;
		PipelineNode pipelineNode = new PipelineNode();
		PluginNode pluginNode = new PluginNode();
		pipelineNode.setPlugin(pluginNode);
		pipelineNode.setName(currentStageName);
		pluginNode.setName("FilterTransform");
		pluginNode.setType("transform");
		pluginNode.setLabel(currentStageName);
		pluginNode.setArtifact(this.featureEngineeringArtifact);
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("filters", windowEndTime.getMillis() + ":LTE(" + columnName + ")," + windowStartTime.getMillis()
				+ ":GTE(" + columnName + ")");
		pluginNode.setProperties(properties);
		Schema lastStageSchema = schemaMap.get(lastStageName);

		pipelineNode.setOutputSchema(gsonObj.toJson(lastStageSchema));

		List<InOutSchema> inputSchema = new LinkedList<InOutSchema>();
		InOutSchema inSchema = new InOutSchema();
		inputSchema.add(inSchema);
		inSchema.setName(lastStageName);
		inSchema.setSchema(gsonObj.toJson(lastStageSchema));
		pipelineNode.setInputSchema(inputSchema);

		stageMap.put(currentStageName, pipelineNode);
		schemaMap.put(currentStageName, lastStageSchema);
		lastStageMapForTable.put(tableName, currentStageName);
		putInConnection(lastStageName, currentStageName);
		isUsedStage.add(currentStageName);
		isUsedStage.add(lastStageName);
	}

	private void createSourceStages(Map<String, NullableSchema> inputDataSourceInfoMap,
			Map<String, CDAPPipelineInfo> wranglerPluginConfigMap, PipelineConfiguration pipeLineConfiguration) {
		for (Map.Entry<String, CDAPPipelineInfo> wranglerPluginEntry : wranglerPluginConfigMap.entrySet()) {
			String tableName = wranglerPluginEntry.getKey();
			for (BasePipelineNode stage : wranglerPluginEntry.getValue().getConfig().getStages()) {
				String stageName = tableName + "_" + stage.getName();
				stage.setName(stageName);
				stage.getPlugin().setLabel(stageName);
				stageMap.put(stageName, stage);
			}
			schemaMap.put(tableName, getSchemaObjectFromNullable(inputDataSourceInfoMap.get(tableName)));
			for (Connection connection : wranglerPluginEntry.getValue().getConfig().getConnections()) {
				String sourceConnectionName = tableName + "_" + connection.from;
				String destinationConnectionName = tableName + "_" + connection.to;
				putInConnection(sourceConnectionName, destinationConnectionName);
				lastStageMapForTable.put(tableName, destinationConnectionName);
				schemaMap.put(destinationConnectionName, schemaMap.get(tableName));
				isUsedStage.add(sourceConnectionName);
				isUsedStage.add(destinationConnectionName);
			}
		}
	}

	private Schema getSchemaObjectFromNullable(NullableSchema nullableSchema) {
		Schema schema = new Schema();
		schema.setName(nullableSchema.getName());
		schema.setType(nullableSchema.getType());
		SchemaFieldName[] schemaFieldName = new SchemaFieldName[nullableSchema.getFields().length];
		int index = 0;
		for (NullableSchemaField field : nullableSchema.getFields()) {
			schemaFieldName[index++] = field;
		}
		schema.setFields(schemaFieldName);
		return schema;
	}

	private PipelineNode getCSVParserStagePipelineNode(Map<String, Object> dataSourceInfo, String csvStageName,
			Schema schema) {
		PipelineNode pipelineNode = new PipelineNode();
		PluginNode pluginNode = new PluginNode();
		pipelineNode.setPlugin(pluginNode);
		pipelineNode.setName(csvStageName);
		pluginNode.setName("CSVParser");
		pluginNode.setType("transform");
		pluginNode.setLabel(csvStageName);
		Artifact artifact = new Artifact();
		artifact.setName("transform-plugins");
		artifact.setScope(this.pluginArtifact.getScope());
		artifact.setVersion(this.pluginArtifact.getVersion());
		pluginNode.setArtifact(artifact);
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("format", "EXCEL");
		properties.put("schema", gsonObj.toJson((Schema) dataSourceInfo.get("schema")));
		properties.put("field", "body");
		pluginNode.setProperties(properties);

		pipelineNode.setOutputSchema(gsonObj.toJson((Schema) dataSourceInfo.get("schema")));

		List<InOutSchema> inputSchema = new LinkedList<InOutSchema>();
		InOutSchema inSchema = new InOutSchema();
		inputSchema.add(inSchema);
		inSchema.setName((String) dataSourceInfo.get("name"));
		inSchema.setSchema(gsonObj.toJson(schema));
		pipelineNode.setInputSchema(inputSchema);
		return pipelineNode;
	}

	private StagePipelineNode getSourceStagePipelineNode(Map<String, Object> dataSourceInfo, Schema schema) {
		StagePipelineNode pipelineNode = new StagePipelineNode();

		PluginNode pluginNode = new PluginNode();
		pipelineNode.setPlugin(pluginNode);
		pipelineNode.setName((String) dataSourceInfo.get("name"));
		pluginNode.setName((String) dataSourceInfo.get("pluginName"));
		pluginNode.setType((String) dataSourceInfo.get("type"));

		pluginNode.setArtifact(this.pluginArtifact);
		Map<String, Object> properties = new HashMap<String, Object>();
		for (Map.Entry<String, Object> entry : dataSourceInfo.entrySet()) {
			if (!specificPluginNames.contains(entry.getKey())) {
				properties.put(entry.getKey(), entry.getValue().toString());
			}
		}
		Schema readSchema = (Schema) dataSourceInfo.get("schema");
		schema.setName(readSchema.getName());
		schema.setType(readSchema.getType());
		SchemaField[] fields = new SchemaField[2];
		fields[0] = new SchemaField();
		fields[1] = new SchemaField();
		fields[0].setName("body");
		fields[0].setType("string");
		fields[1].setName("offset");
		fields[1].setType("long");
		schema.setFields(fields);
		properties.put("schema", gsonObj.toJson(schema));
		pluginNode.setProperties(properties);
		pipelineNode.setOutputSchema(gsonObj.toJson(schema));
		return pipelineNode;
	}

	private PipelineNode getFileSinkNode(String stageName, String referenceName, String fileName,
			String lastStageName) {
		PipelineNode stageNode = new PipelineNode();
		stageNode.setName(stageName);
		PluginNode pluginNode = new PluginNode();
		stageNode.setPlugin(pluginNode);
		pluginNode.setLabel(stageName);
		pluginNode.setName("File");
		pluginNode.setType("batchsink");
		Artifact esArtifact = new Artifact();
		esArtifact.setName("file-plugins");
		esArtifact.setVersion("2.0.0");
		esArtifact.setScope("USER");
		pluginNode.setArtifact(esArtifact);
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("referenceName", referenceName);
		properties.put("path",
				"/Users/bhupesh.goel/Documents/codebase/cdap-codebase/cdap-build/AutoFeatureSynthesis/resources/"
						+ fileName);
		properties.put("suffix", "YYYY-MM-dd-HH-mm");
		pluginNode.setProperties(properties);

		putInConnection(lastStageName, stageName);
		stageMap.put(stageName, stageNode);
		isUsedStage.add(lastStageName);
		isUsedStage.add(stageName);
		return stageNode;
	}

	private PipelineNode getElasticSearchSinkNode(String stageName, String referenceName, String esIndex, String esType,
			String esIdField, String lastStageName) {
		PipelineNode stageNode = new PipelineNode();
		stageNode.setName(stageName);
		PluginNode pluginNode = new PluginNode();
		stageNode.setPlugin(pluginNode);
		pluginNode.setLabel(stageName);
		pluginNode.setName("Elasticsearch");
		pluginNode.setType("batchsink");
		Artifact esArtifact = new Artifact();
		esArtifact.setName("elasticsearch-plugins");
		esArtifact.setVersion("1.8.0");
		esArtifact.setScope("USER");
		pluginNode.setArtifact(esArtifact);
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("referenceName", referenceName);
		properties.put("es.host", "localhost:9200");
		properties.put("es.index", esIndex);
		properties.put("es.type", esType);
		properties.put("es.idField", esIdField);
		pluginNode.setProperties(properties);

		putInConnection(lastStageName, stageName);
		stageMap.put(stageName, stageNode);
		isUsedStage.add(lastStageName);
		isUsedStage.add(stageName);
		return stageNode;
	}

	private PipelineNode getCDAPTableSinkNode(final String stageName, final String lastStageName) {
		PipelineNode stageNode = new PipelineNode();
		stageNode.setName(stageName);
		PluginNode pluginNode = new PluginNode();
		stageNode.setPlugin(pluginNode);
		pluginNode.setLabel(stageName);
		pluginNode.setName("Table");
		pluginNode.setType("batchsink");
		pluginNode.setArtifact(this.pluginArtifact);
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("name", pipelineName);
		properties.put("schema.row.field", "Id");
		properties.put("schema", gsonObj.toJson(getStatsSchema()));
		pluginNode.setProperties(properties);

		putInConnection(lastStageName, stageName);
		stageMap.put(stageName, stageNode);
		isUsedStage.add(lastStageName);
		isUsedStage.add(stageName);
		return stageNode;
	}

	private Object getStatsSchema() {
		Schema schema = new Schema();
		schema.setName("fileRecord");
		schema.setType("record");
		List<SchemaFieldName> schemaFields = new LinkedList<SchemaFieldName>();
		NullableSchemaField field = new NullableSchemaField();
		for (FeatureSTATS stats : FeatureSTATS.values()) {
			field = new NullableSchemaField();
			field.setName(stats.getName());
			field.setNullType(stats.getType());
			schemaFields.add(field);
		}
		SchemaField field2 = new SchemaField();
		field2.setName("Id");
		field2.setType("long");
		schemaFields.add(field2);
		schema.setFields(schemaFields.toArray(new SchemaFieldName[0]));
		return schema;
	}

	private void truncateIslandNodesFromDAG() {
		for (String entity : entityNames) {
			if (entity.equals(targetEntity))
				continue;
			if (isConnectedToReachableNodes(entity))
				continue;
			else
				deleteGraphFromEntityNode(entity);
		}
	}

	private void deleteGraphFromEntityNode(String entity) {
		stageMap.remove(entity);
		schemaMap.remove(entity);
		Set<String> destinations = connections.get(entity);
		connections.remove(entity);
		if (destinations != null) {
			for (String dest : destinations) {
				deleteGraphFromEntityNode(dest);
			}
		}
	}

	private boolean isConnectedToReachableNodes(String entity) {
		if (isNodeReachable.contains(entity))
			return true;
		Set<String> destinations = connections.get(entity);
		if (destinations != null)
			for (String dest : destinations) {
				if (isNodeReachable.contains(dest))
					return true;
				boolean result = isConnectedToReachableNodes(dest);
				if (result)
					return true;
			}
		return false;
	}

	private void markReachableNodesFromTargetEntity(String root) {
		if (!connections.containsKey(root)) {
			return;
		}
		isNodeReachable.add(root);
		Set<String> destinations = connections.get(root);
		if (destinations != null)
			for (String dest : destinations) {
				if (isNodeReachable.contains(dest))
					continue;
				isNodeReachable.add(dest);
				markReachableNodesFromTargetEntity(dest);
			}
	}

	private void generateTrashSinkStage() {
		Set<String> toBeTrashedStages = new HashSet<String>(stageMap.keySet());
		for (String stageName : toBeTrashedStages) {
			if (connections.containsKey(stageName) || isStageSinkType(stageName))
				continue;
			addTrashSinkForStage(stageName);
		}
	}

	private boolean isStageSinkType(String stageName) {
		BasePipelineNode node = stageMap.get(stageName);
		if (node.getPlugin().getType().equals("batchsink"))
			return true;
		return false;
	}

	private PipelineNode addTrashSinkForStage(String lastStageName) {
		String currentStageName = "Trash_" + lastStageName;
		PipelineNode trashStage = new PipelineNode();
		trashStage.setName(currentStageName);
		PluginNode pluginNode = new PluginNode();
		trashStage.setPlugin(pluginNode);
		pluginNode.setLabel(trashStage.getName());
		pluginNode.setName("Trash");
		pluginNode.setType("batchsink");
		Artifact esArtifact = new Artifact();
		esArtifact.setName("trash-plugin");
		esArtifact.setVersion("1.0.0");
		esArtifact.setScope("USER");
		pluginNode.setArtifact(esArtifact);
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("referenceName", "Trash");
		pluginNode.setProperties(properties);

		putInConnection(lastStageName, currentStageName);
		stageMap.put(currentStageName, trashStage);
		isUsedStage.add(currentStageName);
		return trashStage;
	}

	private void populateStagesFromOperations(String sourceTables, String operations) {
		if (sourceTables.contains("->")) {
			populateStagesFromGroupByOperations(sourceTables, operations);
		} else {
			populateStagesFromTransformOperations(sourceTables, operations);
		}
	}

	private void populateStagesFromGroupByOperations(String sourceTables, String operations) {
		String tokens[] = sourceTables.split("->");
		String sourceTable = tokens[0];
		String destTable = tokens[1];
		String sourceTableLastStage = lastStageMapForTable.get(sourceTable);
		String destTableLastStage = lastStageMapForTable.get(destTable);
		if (!operations.startsWith("[") || !operations.endsWith("]")) {
			return;
		}
		operations = operations.substring(1, operations.length() - 1).trim();
		tokens = operations.split(", ");
		if (tokens.length < 3) {
			throw new IllegalStateException("Input Operation Data is not complete");
		}
		String tokens2[] = tokens[1].substring(1, tokens[1].length() - 1).split("::");
		String sourceJoinKey = "";
		String destJoinKey = "";
		for (int i = 0; i < 2; i++) {
			String token3[] = tokens2[i].split("\\.");
			if (token3[0].equals(sourceTable))
				sourceJoinKey = token3[1];
			else
				destJoinKey = token3[1];
		}
		if (tokens[0].equals("'Transformation'")) {
			generateJoinerForTransformation(sourceTableLastStage, destTableLastStage, sourceJoinKey, destJoinKey,
					tokens, destTable, sourceTable);
		} else if (tokens[0].equals("'Group_By'")) {
			List<String> currentStageGroupByTempTableNames = generateGroupByOnSourceTable(sourceTable,
					sourceTableLastStage, tokens, sourceJoinKey);
			// Take join of all temporary group by tables.
			String sourceTempTableName = takejoinOfAllTempTables(currentStageGroupByTempTableNames, sourceJoinKey);
			String stageName = takePlainJoinOfTwoTables(sourceTempTableName, destTableLastStage, sourceJoinKey,
					destJoinKey);
			lastStageMapForTable.put(destTable, stageName);
		}
	}

	private String takejoinOfAllTempTables(List<String> currentStageTempTableNames, String sourceJoinKey) {
		if (currentStageTempTableNames.isEmpty())
			return null;
		String lastJoinerTempTableName = currentStageTempTableNames.get(0);
		for (int index = 1; index < currentStageTempTableNames.size(); index++) {
			String destTableLastStage = lastJoinerTempTableName;
			String sourceTableLastStage = currentStageTempTableNames.get(index);
			lastJoinerTempTableName = takePlainJoinOfTwoTables(sourceTableLastStage, destTableLastStage, sourceJoinKey,
					sourceJoinKey);
		}
		return lastJoinerTempTableName;
	}

	private String takePlainJoinOfTwoTables(String sourceTableLastStage, String destTableLastStage,
			String sourceJoinKey, String destJoinKey) {
		// String stageName = destTableLastStage + "_joiner_" + sourceTableLastStage;
		String stageName = "";
		if (this.generatedStageMap.containsKey(destTableLastStage)) {
			stageName = this.generatedStageMap.get(destTableLastStage) + "_joiner_";
		} else
			stageName = destTableLastStage + "_joiner_";
		if (this.generatedStageMap.containsKey(sourceTableLastStage)) {
			stageName += this.generatedStageMap.get(sourceTableLastStage);
		} else
			stageName += sourceTableLastStage;

		stageName = getNextUniqueIdForStage(stageName);
		stageName = "Joiner_" + stageName;
		PipelineNode pipelineNode = new PipelineNode();
		pipelineNode.setName(stageName);
		PluginNode pluginNode = new PluginNode();
		pipelineNode.setPlugin(pluginNode);
		pluginNode.setArtifact(this.featureEngineeringArtifact);
		pluginNode.setName("JoinerDynamic");
		pluginNode.setType("batchjoiner");
		pluginNode.setLabel(stageName);
		Map<String, Object> pluginProperties = new HashMap<String, Object>();
		pluginNode.setProperties(pluginProperties);

		pluginProperties.put("joinKeys",
				sourceTableLastStage + "." + sourceJoinKey + " = " + destTableLastStage + "." + destJoinKey);
		pluginProperties.put("requiredInputs", sourceTableLastStage + ", " + destTableLastStage);
		pluginProperties.put("selectedFields", ",");
		pluginProperties.put("keysToBeAppended", ",");
		if (enablePartitions)
			pluginProperties.put("numPartitions", NUM_PARTITIONS);
		stageMap.put(stageName, pipelineNode);
		putInConnection(sourceTableLastStage, stageName);
		putInConnection(destTableLastStage, stageName);
		isUsedStage.add(sourceTableLastStage);
		isUsedStage.add(stageName);
		isUsedStage.add(destTableLastStage);

		return stageName;
	}

	private String createTempTransformStage(String tableName, String csvStageName) {
		PipelineNode pipelineNode = new PipelineNode();
		String stageName = csvStageName + "Temp_Transform_" + System.currentTimeMillis();
		pipelineNode.setName(stageName);
		PluginNode pluginNode = new PluginNode();
		pipelineNode.setPlugin(pluginNode);
		pluginNode.setArtifact(this.featureEngineeringArtifact);
		pluginNode.setName("RowTransform");
		pluginNode.setType("transform");
		pluginNode.setLabel(stageName);
		Map<String, Object> pluginProperties = new HashMap<String, Object>();
		pluginNode.setProperties(pluginProperties);
		pluginProperties.put("primitives", "");
		pluginProperties.put("isDynamicSchema", true);
		putInConnection(csvStageName, stageName);
		isUsedStage.add(csvStageName);
		isUsedStage.add(stageName);
		stageMap.put(stageName, pipelineNode);
		if (tableName != null || !tableName.isEmpty())
			lastStageMapForTable.put(tableName, stageName);
		return stageName;
	}

	private List<String> generateGroupByOnSourceTable(String sourceTable, String sourceTableLastStage, String[] tokens,
			String sourceJoinKey) {
		List<String> currentStageGroupByTempTableNames = new LinkedList<String>();
		Map<PluginSummary, List<String>> pluginFunctionMap = getPluginFunctionMap(aggregatePluginFunctionMap, tokens,
				2);
		for (Map.Entry<PluginSummary, List<String>> entry : pluginFunctionMap.entrySet()) {
			PluginSummary pluginSummary = entry.getKey();
			List<String> columnOperations = entry.getValue();
			String stageName = createAndAddTemporaryGroupByStage(pluginSummary, columnOperations, sourceTable,
					sourceTableLastStage, sourceJoinKey);
			currentStageGroupByTempTableNames.add(stageName);
		}
		return currentStageGroupByTempTableNames;
	}

	private String createAndAddTemporaryGroupByStage(PluginSummary pluginSummary, List<String> columnOperations,
			String sourceTable, String sourceTableLastStage, String sourceJoinKey) {
		PipelineNode pipelineNode = new PipelineNode();
		String stageName = "";
		if (this.generatedStageMap.containsKey(sourceTableLastStage)) {
			stageName = this.generatedStageMap.get(sourceTableLastStage);
		} else
			stageName = sourceTableLastStage;
		stageName += "_" + pluginSummary.getName() + "_" + System.currentTimeMillis();
		stageName = getNextUniqueIdForStage(stageName);
		stageName = "GroupByAggregate_" + stageName;
		pipelineNode.setName(stageName);
		PluginNode pluginNode = new PluginNode();
		pipelineNode.setPlugin(pluginNode);
		pluginNode.setArtifact(this.featureEngineeringArtifact);
		pluginNode.setName(pluginSummary.getName());
		pluginNode.setType(pluginSummary.getType());
		pluginNode.setLabel(stageName);
		Map<String, Object> pluginProperties = new HashMap<String, Object>();
		pluginNode.setProperties(pluginProperties);
		if (pluginSummary.getName().equals("GroupByAggregateFE")) {
			createGroupByAggregateStage(pipelineNode, pluginNode, columnOperations, sourceJoinKey, pluginProperties,
					pluginSummary, sourceTableLastStage);
		} else if (pluginSummary.getName().equals("GroupByCategoricalAggregate")) {
			createGroupByCategoricalAggregateStage(pipelineNode, pluginNode, columnOperations, sourceJoinKey,
					pluginProperties, pluginSummary, sourceTableLastStage, sourceTable);
		}
		stageMap.put(stageName, pipelineNode);
		putInConnection(sourceTableLastStage, stageName);
		isUsedStage.add(sourceTableLastStage);
		isUsedStage.add(stageName);
		return stageName;
	}

	private void computeManualyProvidedAggVariables() {
		for (Map.Entry<String, Map<String, List<String>>> entry : this.appliedAggFunctionsWithArguments.entrySet()) {
			String tableName = entry.getKey();
			Map<String, List<String>> columnDataTypeMap = entry.getValue();
			if (columnDataTypeMap.isEmpty())
				continue;
			String sourceJoinKey = "";
			String destTable = "";
			String destJoinKey = "";
			String lastStageName = this.lastStageMapForTable.get(tableName);
			Map<String, Map<String, List<String>>> pluginColumnDataTypeMap = new HashMap<>();
			Map<String, PluginSummary> pluginSummaryMap = new HashMap<>();
			getPluginColumnDataTyepMap(columnDataTypeMap, pluginColumnDataTypeMap, pluginSummaryMap,
					this.multiInputAggregatePluginFunctionMap);
			List<String> currentStageMultiInputGroupByTempTableNames = new LinkedList<String>();
			for (Map.Entry<String, Map<String, List<String>>> entry2 : pluginColumnDataTypeMap.entrySet()) {
				PluginSummary pluginSummary = pluginSummaryMap.get(entry2.getKey());
				PipelineNode pipelineNode = new PipelineNode();
				String currentStageName = "";
				if (this.generatedStageMap.containsKey(lastStageName)) {
					currentStageName = this.generatedStageMap.get(lastStageName);
				} else
					currentStageName = lastStageName;
				currentStageName += "_" + entry2.getKey() + "_" + System.currentTimeMillis();
				currentStageName = getNextUniqueIdForStage(currentStageName);
				currentStageName = entry2.getKey() + "_" + currentStageName;
				currentStageMultiInputGroupByTempTableNames.add(currentStageName);
				pipelineNode.setName(currentStageName);
				PluginNode pluginNode = new PluginNode();
				pipelineNode.setPlugin(pluginNode);
				pluginNode.setArtifact(this.featureEngineeringArtifact);
				pluginNode.setName(pluginSummary.getName());
				pluginNode.setType(pluginSummary.getType());
				pluginNode.setLabel(currentStageName);
				Map<String, Object> pluginProperties = new HashMap<String, Object>();
				pluginNode.setProperties(pluginProperties);
				if (pluginSummary.getName().equals("GroupByCategoricalAggregate")) {
					createGroupByMultiInputCategoricalAggregateStage(pipelineNode, pluginNode, entry2.getValue(),
							pluginProperties, pluginSummary, lastStageName, tableName);
				}
				stageMap.put(currentStageName, pipelineNode);
				putInConnection(lastStageName, currentStageName);
				isUsedStage.add(lastStageName);
				isUsedStage.add(currentStageName);
				sourceJoinKey = entry2.getValue().values().iterator().next().get(3);
				destTable = entry2.getValue().values().iterator().next().get(4);
				destJoinKey = entry2.getValue().values().iterator().next().get(5);
			}
			String sourceTempTableName = takejoinOfAllTempTables(currentStageMultiInputGroupByTempTableNames,
					sourceJoinKey);

			String stageName = takePlainJoinOfTwoTables(sourceTempTableName, lastStageMapForTable.get(destTable),
					sourceJoinKey, destJoinKey);
			lastStageMapForTable.put(destTable, stageName);
		}
	}

	private void createGroupByMultiInputCategoricalAggregateStage(PipelineNode pipelineNode, PluginNode pluginNode,
			Map<String, List<String>> toBeComputedColumnTypeMap, Map<String, Object> pluginProperties,
			PluginSummary pluginSummary, String lastStageName, String tableName) {

		String sourceJoinKey = toBeComputedColumnTypeMap.values().iterator().next().get(3);
		if (sourceJoinKey == null) {
			throw new IllegalStateException("Group By Key not found in table " + tableName
					+ " for createGroupByMultiInputCategoricalAggregateStage operation");
		}

		StringBuilder aggregates = new StringBuilder();
		int index = 0;

		for (Map.Entry<String, List<String>> entry : toBeComputedColumnTypeMap.entrySet()) {
			String[] token = new String[3];
			token[1] = entry.getKey();
			token[0] = "";
			token[2] = entry.getValue().get(0);
			token[0] = entry.getValue().get(1).trim();
			if (index > 0) {
				aggregates.append(",");
			}
			aggregates.append(token[1] + ":" + token[2].toUpperCase() + "(" + token[0] + ")");
			String outputType = entry.getValue().get(2);
			categoricalColumnsToBeChecked.put(token[1].toLowerCase(), null);
			index++;
		}

		pluginProperties.put("aggregates", aggregates.toString());
		if (enablePartitions)
			pluginProperties.put("numPartitions", NUM_PARTITIONS);
		pluginProperties.put("groupByFields", sourceJoinKey);
		pluginProperties.put("isDynamicSchema", true);
	}

	private void createGroupByCategoricalAggregateStage(PipelineNode pipelineNode, PluginNode pluginNode,
			List<String> columnOperations, String sourceJoinKey, Map<String, Object> pluginProperties,
			PluginSummary pluginSummary, String sourceTableLastStage, String sourceTable) {

		StringBuilder aggregates = new StringBuilder();
		int index = 0;
		for (String columnOperation : columnOperations) {
			String token[] = columnOperation.split(",");
			if (index > 0) {
				aggregates.append(",");
			}
			aggregates.append(token[1] + ":" + token[2].toUpperCase() + "(" + token[0] + ")");
			Set<String> columnSet = categoricalColumnMap.get(sourceTable);
			if (columnSet == null) {
				continue;
			}
			if (!columnSet.contains(token[0].toLowerCase())) {
				throw new IllegalStateException(
						"Column is not categorical for column " + sourceTable + "." + token[0].toLowerCase());
			}
			categoricalColumnsToBeChecked.put(token[1].toLowerCase(), null);

			index++;
		}
		pluginProperties.put("aggregates", aggregates.toString());
		if (enablePartitions)
			pluginProperties.put("numPartitions", NUM_PARTITIONS);
		pluginProperties.put("groupByFields", sourceJoinKey);
		pluginProperties.put("isDynamicSchema", true);
	}

	private String getSchemaType(SchemaFieldName field) {
		if (field instanceof SchemaField) {
			return ((SchemaField) field).getType();
		} else if (field instanceof NullableSchemaField) {
			return ((NullableSchemaField) field).getType().get(0);
		}
		return null;
	}

	private String getOutputTypeFromListType(String outputType) {
		int indexSt = outputType.indexOf('<');
		int indexEnd = outputType.indexOf('>');
		return outputType.substring(indexSt + 1, indexEnd);
	}

	private void createGroupByAggregateStage(PipelineNode pipelineNode, PluginNode pluginNode,
			List<String> columnOperations, String sourceJoinKey, Map<String, Object> pluginProperties,
			PluginSummary pluginSummary, String sourceTableLastStage) {

		StringBuilder aggregates = new StringBuilder();
		int index = 0;

		for (String columnOperation : columnOperations) {
			String token[] = columnOperation.split(",");
			if (index > 0) {
				aggregates.append(",");
			}
			aggregates.append(token[1] + ":" + token[2].toUpperCase() + "(" + token[0] + ")");
			index++;
		}
		pluginProperties.put("aggregates", aggregates.toString());
		pluginProperties.put("groupByFields", sourceJoinKey);
		pluginProperties.put("isDynamicSchema", true);
		if (enablePartitions)
			pluginProperties.put("numPartitions", NUM_PARTITIONS);
	}

	private void generateJoinerForTransformation(String sourceTableLastStage, String destTableLastStage,
			String sourceJoinKey, String destJoinKey, String[] tokens, String destTable, String sourceTable) {

		PipelineNode pipelineNode = new PipelineNode();
		// String stageName = destTableLastStage + "_joiner_" + sourceTableLastStage;
		String stageName = "";
		if (this.generatedStageMap.containsKey(destTableLastStage)) {
			stageName = this.generatedStageMap.get(destTableLastStage) + "_joiner_";
		} else
			stageName = destTableLastStage + "_joiner_";
		if (this.generatedStageMap.containsKey(sourceTableLastStage)) {
			stageName += this.generatedStageMap.get(sourceTableLastStage);
		} else
			stageName += sourceTableLastStage;
		stageName = getNextUniqueIdForStage(stageName);
		stageName = "Joiner_" + stageName;
		pipelineNode.setName(stageName);
		PluginNode pluginNode = new PluginNode();
		pipelineNode.setPlugin(pluginNode);
		pluginNode.setArtifact(this.featureEngineeringArtifact);
		pluginNode.setName("JoinerDynamic");
		pluginNode.setType("batchjoiner");
		pluginNode.setLabel(stageName);
		Map<String, Object> pluginProperties = new HashMap<String, Object>();
		pluginNode.setProperties(pluginProperties);
		int index = 0;
		StringBuilder selectedFields = new StringBuilder();
		for (int i = 2; i < tokens.length; i++) {
			String tokens2[] = tokens[i].substring(1, tokens[i].length() - 1).trim().split(",");
			if (tokens2.length != 2) {
				throw new IllegalStateException("Input operations for direct relation has wrong input " + tokens[i]);
			}

			if (index > 0)
				selectedFields.append(",");
			selectedFields.append(
					sourceTableLastStage + "." + normalizeString(tokens2[0]) + " as " + normalizeString(tokens2[1]));
			index++;
		}
		pluginProperties.put("joinKeys",
				sourceTableLastStage + "." + sourceJoinKey + " = " + destTableLastStage + "." + destJoinKey);
		pluginProperties.put("requiredInputs", sourceTableLastStage + ", " + destTableLastStage);
		pluginProperties.put("selectedFields", selectedFields.toString());
		pluginProperties.put("keysToBeAppended", ",");
		if (enablePartitions)
			pluginProperties.put("numPartitions", NUM_PARTITIONS);

		stageMap.put(stageName, pipelineNode);
		putInConnection(sourceTableLastStage, stageName);
		putInConnection(destTableLastStage, stageName);
		isUsedStage.add(sourceTableLastStage);
		isUsedStage.add(stageName);
		isUsedStage.add(destTableLastStage);
		lastStageMapForTable.put(destTable, stageName);
	}

	private Map<String, String> getDataTypeMap(SchemaFieldName[] fields) {
		Map<String, String> dataTypeMap = new HashMap<String, String>();
		for (SchemaFieldName field : fields) {
			dataTypeMap.put(field.getName(), getSchemaType(field));
		}
		return dataTypeMap;
	}

	private void populateStagesFromTransformOperations(String sourceTables, String operations) {
		if (!operations.startsWith("[") || !operations.endsWith("]")) {
			return;
		}
		operations = operations.substring(1, operations.length() - 1).trim();
		String[] tokens = operations.split(", ");
		if (!tokens[0].equals("'Transformation'")) {
			return;
		}
		Map<PluginSummary, List<String>> pluginFunctionMap = getPluginFunctionMap(transformPluginFunctionMap, tokens,
				1);

		for (Map.Entry<PluginSummary, List<String>> entry : pluginFunctionMap.entrySet()) {
			PluginSummary pluginSummary = entry.getKey();
			List<String> columnOperations = entry.getValue();
			createAndAddTransformStage(pluginSummary, columnOperations, sourceTables);
		}
	}

	private Map<PluginSummary, List<String>> getPluginFunctionMap(Map<String, PluginSummary> pluginFunctionMap,
			String[] tokens, int startingIndex) {
		Map<PluginSummary, List<String>> pluginFunctionListMap = new HashMap<PluginSummary, List<String>>();
		for (int i = startingIndex; i < tokens.length; i++) {
			String[] columnOperation = tokens[i].substring(1, tokens[i].length() - 1).split(",");
			String sourceColumn = normalizeString(columnOperation[0]);
			String desColumn = normalizeString(columnOperation[1]);
			String function = columnOperation[2].toLowerCase();
			PluginSummary pluginSummary = pluginFunctionMap.get(function);
			List<String> pluginFunctions = pluginFunctionListMap.get(pluginSummary);
			if (pluginFunctions == null) {
				pluginFunctions = new LinkedList<String>();
				pluginFunctionListMap.put(pluginSummary, pluginFunctions);
			}
			pluginFunctions.add(sourceColumn + "," + desColumn + "," + function);
		}
		return pluginFunctionListMap;
	}

	private void createAndAddTransformStage(PluginSummary pluginSummary, List<String> columnOperations,
			String sourceTables) {
		PipelineNode pipelineNode = new PipelineNode();
		String stageName = "";
		if (this.generatedStageMap.containsKey(lastStageMapForTable.get(sourceTables))) {
			stageName = this.generatedStageMap.get(lastStageMapForTable.get(sourceTables)) + "_";
		} else
			stageName = lastStageMapForTable.get(sourceTables) + "_";
		stageName += pluginSummary.getName();

		stageName = getNextUniqueIdForStage(stageName);
		stageName = "Transformation_" + stageName;
		pipelineNode.setName(stageName);
		PluginNode pluginNode = new PluginNode();
		pipelineNode.setPlugin(pluginNode);
		pluginNode.setArtifact(this.featureEngineeringArtifact);
		pluginNode.setName(pluginSummary.getName());
		pluginNode.setType(pluginSummary.getType());
		pluginNode.setLabel(stageName);
		Map<String, Object> pluginProperties = new HashMap<String, Object>();
		pluginNode.setProperties(pluginProperties);
		if (pluginSummary.getName().equals("RowTransform")) {
			StringBuilder primitives = new StringBuilder();
			int index = 0;
			for (String columnOperation : columnOperations) {
				String token[] = columnOperation.split(",");
				token[0] = token[0].toLowerCase();

				if (index > 0) {
					primitives.append(",");
				}
				primitives.append(token[1] + ":" + token[2].toUpperCase() + "(" + token[0] + ")");
				index++;
			}
			pluginProperties.put("primitives", primitives.toString());
			pluginProperties.put("isDynamicSchema", true);
			if (enablePartitions)
				pluginProperties.put("numPartitions", NUM_PARTITIONS);
		}
		putInConnection(lastStageMapForTable.get(sourceTables), stageName);
		isUsedStage.add(lastStageMapForTable.get(sourceTables));
		isUsedStage.add(stageName);
		stageMap.put(stageName, pipelineNode);
		lastStageMapForTable.put(sourceTables, stageName);
	}

	private String toCDAPConfig(Map<String, Set<String>> categoricalColumnsToBeChecked2) {
		StringBuilder sb = new StringBuilder();
		int index = 0;
		for (String column : categoricalColumnsToBeChecked2.keySet()) {
			if (index > 0)
				sb.append(",");
			sb.append(column);
			index++;
		}
		return sb.toString();
	}

	private List<SchemaFieldName> getAllMatchingSchemaFields(SchemaFieldName[] lastSchemaFields, String inputSchema) {
		List<SchemaFieldName> fields = new LinkedList<SchemaFieldName>();
		for (SchemaFieldName field : lastSchemaFields) {
			if (field.getName().contains(inputSchema)) {
				fields.add(field);
			}
		}
		return fields;
	}

	private void setInputOutputSchema(Schema lastSchema, Schema currentSchema, String lastStageName,
			PipelineNode pipelineNode) {
		generateOutputSchema("etlSchemaBody", currentSchema, pipelineNode);
		List<InOutSchema> inputSchemaList = generateInputSchema(lastStageName, lastSchema, pipelineNode);
		pipelineNode.setInputSchema(inputSchemaList);
	}

	private void generateOutputSchema(String outputName, Schema outputSchema, PipelineNode pipelineNode) {
		pipelineNode.setOutputSchema(gsonObj.toJson(outputSchema));
	}

	private List<InOutSchema> generateInputSchema(String inputName, Schema inputSchema, PipelineNode pipelineNode) {
		List<InOutSchema> inputSchemaList = new LinkedList<InOutSchema>();
		InOutSchema inSchema = new InOutSchema();
		inSchema.setName(inputName);
		inSchema.setSchema(gsonObj.toJson(inputSchema));
		inputSchemaList.add(inSchema);
		return inputSchemaList;
	}

	private String getOutputType(String inputDataType, String function, String pluginName,
			Map<String, Map<String, Map<String, String>>> functionDataTypeInfoMap) {
		Map<String, String> inputOutputDataType = functionDataTypeInfoMap.get(pluginName).get(function);
		String outputType = inputOutputDataType.get("*");
		if (outputType != null) {
			if (outputType.equals("same"))
				return inputDataType;
			else {
				return outputType.toLowerCase();
			}
		}

		outputType = inputOutputDataType.get(inputDataType);
		if (outputType == null) {
			throw new IllegalStateException("Output data type not found for Input data type.=" + inputDataType);
		}
		if (outputType.equals("same")) {
			return inputDataType;
		}
		return outputType;
	}

	private String normalizeString(final String input) {
		return input.replaceAll("[\\.()]", "_").toLowerCase();
	}

	public static void main(String args[]) {
		System.out.println("tt_wd.".replaceAll("[\\.()]", "_").toLowerCase());
		List<String> tt = new LinkedList<String>();
		tt.add("s");
		tt.add("S");
		Collections.sort(tt);
		System.out.println(tt);
		for (int i = 0; i < 10; i++) {
			int length = 10;
			boolean useLetters = true;
			boolean useNumbers = false;
			System.out.println("Random String=" + RandomStringUtils.random(length, useLetters, useNumbers));
		}
		Schema schema = new Schema();
		schema.setName("etlSchemaBody");
		schema.setType("record");
		SchemaField[] fields = new SchemaField[2];
		schema.setFields(fields);
		fields[0] = new SchemaField();
		fields[1] = new SchemaField();
		fields[0].setName("ts");
		fields[0].setType("long");
		fields[1].setName("body");
		fields[1].setType("string");

		Map<String, String> tmp = new HashMap<String, String>();
		tmp.put("name", "myTable");
		tmp.put("schema", gsonObj.toJson(schema));
		tmp.put("schema.row.field", "ts");

		// System.out.println(gsonObj.toJson(tmp));

		List<Connection> connections = new LinkedList<Connection>();
		Connection con1 = new Connection();
		con1.setFrom("twitterSource");
		con1.setTo("dropProjector");
		connections.add(con1);

		Connection con2 = new Connection();
		con2.setFrom("dropProjector");
		con2.setTo("tableSink");
		connections.add(con2);
		// System.out.println(gsonObj.toJson(connections));

		PluginNode plugin = new PluginNode();
		plugin.setLabel("label");
		plugin.setName("name");
		BasePipelineNode node = new StagePipelineNode();
		node.setName("name1");
		node.setPlugin(plugin);
		((StagePipelineNode) node).setOutputSchema("outputschema");
		BasePipelineNode node2 = new PipelineNode();
		node2.setPlugin(plugin);
		List<InOutSchema> inputSchema = new LinkedList<InOutSchema>();
		InOutSchema ss = new InOutSchema();
		inputSchema.add(ss);
		ss.setName("InOutName");
		ss.setSchema("InOutSchema");
		((PipelineNode) node2).setInputSchema(inputSchema);
		System.out.println("StageNode = " + gsonObj.toJson(node));
		System.out.println("PipelineNode = " + gsonObj.toJson(node2));
	}

}
