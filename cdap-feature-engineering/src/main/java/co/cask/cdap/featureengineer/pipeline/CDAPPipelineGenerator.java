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

import co.cask.cdap.feature.selection.CDAPSubDagGenerator.CDAPSubDagGeneratorOutput;
import co.cask.cdap.featureengineer.pipeline.pojo.Artifact;
import co.cask.cdap.featureengineer.pipeline.pojo.BasePipelineNode;
import co.cask.cdap.featureengineer.pipeline.pojo.CDAPPipelineInfo;
import co.cask.cdap.featureengineer.pipeline.pojo.Connection;
import co.cask.cdap.featureengineer.pipeline.pojo.InOutSchema;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchema;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchemaField;
import co.cask.cdap.featureengineer.pipeline.pojo.PipelineConfiguration;
import co.cask.cdap.featureengineer.pipeline.pojo.PipelineNode;
import co.cask.cdap.featureengineer.pipeline.pojo.PluginNode;
import co.cask.cdap.featureengineer.pipeline.pojo.Schema;
import co.cask.cdap.featureengineer.pipeline.pojo.SchemaField;
import co.cask.cdap.featureengineer.pipeline.pojo.SchemaFieldName;
import co.cask.cdap.featureengineer.pipeline.pojo.StagePipelineNode;
import co.cask.cdap.featureengineer.request.pojo.Relation;
import co.cask.cdap.featureengineer.request.pojo.SchemaColumn;
import co.cask.cdap.proto.artifact.PluginSummary;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author bhupesh.goel
 *
 */
public class CDAPPipelineGenerator {
    
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();
    
    private static final Logger LOG = LoggerFactory.getLogger(CDAPPipelineGenerator.class);
    
    private static final Gson GSON_OBJ = new GsonBuilder().setPrettyPrinting().create();
    private final Map<String, BasePipelineNode> stageMap;
    private final Map<String, Schema> schemaMap;
    private final Map<String, Set<String>> connections;
    private final Set<String> isNodeReachable;
    
    private final Artifact systemArtifact;
    private final Artifact pluginArtifact;
    private final Artifact featureEngineeringArtifact;
    private final Set<String> specificPluginNames;
    private final String featureSelectionPipeline;
    Map<String, PluginSummary> aggregatePluginFunctionMap;
    Map<String, PluginSummary> transformPluginFunctionMap;
    Map<String, String> lastStageMapForTable;
    Map<String, String> firstStageMapForTable;
    List<String> entityNames;
    Map<String, Map<String, Map<String, String>>> functionDataTypeInfoMapTransform;
    Map<String, Map<String, Map<String, String>>> functionDataTypeInfoMapAggregate;
    Set<String> isUsedStage;
    String targetEntity;
    String targetEntityIdField;
    Map<String, Map<String, Set<String>>> categoricalColumnDictionaryMap;
    List<String> categoricalColumns;
    DateTime windowEndTime;
    List<Integer> trainingWindows;
    List<SchemaColumn> timeIndexColumns;
    Map<String, Set<String>> categoricalColumnsToBeChecked;
    private List<Relation> relationShips;
    Map<String, PluginSummary> multiInputAggregatePluginFunctionMap;
    Map<String, PluginSummary> multiInputTransformPluginFunctionMap;
    Map<String, Map<String, List<String>>> appliedAggFunctionsWithArguments;
    Map<String, Map<String, List<String>>> appliedTransFunctionsWithArguments;
    Map<String, String> tableIndexMap;
    Map<String, Set<String>> featureDagEntryDictionaryMap;
    Map<String, CDAPSubDagGeneratorOutput> dagGeneratorOutputMap;
    private List<SchemaColumn> createEntities;
    private Map<String, String> createEntitiesMap;
    private int globalUniqueID;
    private static final String STAGE_NAME = "stage";
    private final Map<String, String> generatedStageMap;
    private final Map<String, String> generatedReverseStageMap;
    
    private String targetEntityStageName;
    
    private static final String NUM_PARTITIONS = "5";
    private static final boolean enablePartitions = false;
    
    /**
     * 
     * @param aggregatePluginFunctionMap2
     * @param transformPluginFunctionMap2
     * @param entityNames2
     * @param targetEntity2
     * @param targetEntityFieldId
     * @param windowEndTime2
     * @param trainingWindows2
     * @param timeIndexColumns2
     * @param multiInputAggregatePluginFunctionMap2
     * @param multiInputTransformPluginFunctionMap2
     * @param appliedAggFunctionsWithArguments2
     * @param appliedTransFunctionsWithArguments2
     * @param indexes
     * @param createEntities
     * @param dagGeneratorOutputMap2
     */
    public CDAPPipelineGenerator(Map<String, PluginSummary> aggregatePluginFunctionMap,
            Map<String, PluginSummary> transformPluginFunctionMap, List<String> entityNames, String targetEntity,
            String targetEntityFieldId, String windowEndTime, List<Integer> trainingWindows,
            List<SchemaColumn> timeIndexColumns, Map<String, PluginSummary> multiInputAggregatePluginFunctionMap,
            Map<String, PluginSummary> multiInputTransformPluginFunctionMap,
            Map<String, Map<String, List<String>>> appliedAggFunctionsWithArguments,
            Map<String, Map<String, List<String>>> appliedTransFunctionsWithArguments, List<SchemaColumn> indexes,
            Map<String, CDAPSubDagGeneratorOutput> dagGeneratorOutputMap, String featureSelectionPipeline,
            List<Relation> relationShips, List<SchemaColumn> createEntities) {
        this.stageMap = new LinkedHashMap<String, BasePipelineNode>();
        this.connections = new LinkedHashMap<String, Set<String>>();
        this.systemArtifact = new Artifact();
        this.pluginArtifact = new Artifact();
        this.featureEngineeringArtifact = new Artifact();
        this.isNodeReachable = new HashSet<String>();
        this.aggregatePluginFunctionMap = aggregatePluginFunctionMap;
        this.transformPluginFunctionMap = transformPluginFunctionMap;
        this.lastStageMapForTable = new HashMap<String, String>();
        this.firstStageMapForTable = new HashMap<String, String>();
        this.entityNames = entityNames;
        for (String entity : entityNames) {
            lastStageMapForTable.put(entity, entity);
        }
        this.createEntities = createEntities;
        this.createEntitiesMap = new HashMap<>();
        if (this.createEntities != null) {
            for (SchemaColumn col : createEntities) {
                createEntitiesMap.put(col.getTable(), col.getColumn());
            }
        }
        this.relationShips = relationShips;
        this.featureSelectionPipeline = featureSelectionPipeline;
        this.dagGeneratorOutputMap = dagGeneratorOutputMap;
        // this.featureDagEntryDictionaryMap = featureDagEntryDictionaryMap;
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
        systemArtifact.setVersion("5.1.0");
        pluginArtifact.setName("core-plugins");
        pluginArtifact.setScope("SYSTEM");
        pluginArtifact.setVersion("2.1.1-SNAPSHOT");
        featureEngineeringArtifact.setName("feature-engineering-plugin");
        featureEngineeringArtifact.setScope("SYSTEM");
        featureEngineeringArtifact.setVersion("2.1.1-SNAPSHOT");
        specificPluginNames = new HashSet<String>();
        specificPluginNames.add("name");
        specificPluginNames.add("type");
        specificPluginNames.add("schema");
        specificPluginNames.add("pluginName");
        isUsedStage = new HashSet<String>();
        this.targetEntity = targetEntity;
        this.targetEntityIdField = targetEntityFieldId;
        this.categoricalColumns = new LinkedList<String>();
        // generateCatetgoricalColumnDictionary(categoricalColumnDictionary);
        if (windowEndTime != null) {
            this.windowEndTime = FORMATTER.parseDateTime(windowEndTime);
        }
        this.trainingWindows = trainingWindows;
    }
    
    private String getNextUniqueIdForStage(final String stage) {
        globalUniqueID++;
        String uniqueStageId = STAGE_NAME + "_" + globalUniqueID;
        this.generatedStageMap.put(uniqueStageId, stage);
        this.generatedReverseStageMap.put(stage, uniqueStageId);
        return uniqueStageId;
    }
    
    private Map<String, Map<String, Set<String>>> generateCatetgoricalColumnDictionary(
            List<String> categoricalColumnDictionary) {
        this.categoricalColumnDictionaryMap = new HashMap<String, Map<String, Set<String>>>();
        if (categoricalColumnDictionary != null) {
            for (String column : categoricalColumnDictionary) {
                String[] tokens = column.split("\\*");
                this.categoricalColumns.add(tokens[0].toLowerCase());
                String[] columnTok = tokens[0].split("\\.");
                String[] dictionary = tokens[1].split(";");
                Map<String, Set<String>> columnDictionary = categoricalColumnDictionaryMap.get(columnTok[0]);
                if (columnDictionary == null) {
                    columnDictionary = new HashMap<String, Set<String>>();
                    categoricalColumnDictionaryMap.put(columnTok[0], columnDictionary);
                }
                Set<String> dictionarySet = columnDictionary.get(columnTok[1]);
                if (dictionarySet == null) {
                    dictionarySet = new HashSet<String>();
                    columnDictionary.put(columnTok[1], dictionarySet);
                }
                for (String dic : dictionary) {
                    dictionarySet.add(dic);
                }
            }
        }
        return this.categoricalColumnDictionaryMap;
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
                        if (input.equals("datetime")) {
                            input = "string";
                        }
                        inputOutputMap.put(input, tokens[0]);
                    }
                } else {
                    tokens = outputTypes.split(":");
                    String[] tokens2 = inputTypes.split(":");
                    if (tokens.length != tokens2.length) {
                        throw new IllegalStateException("Function Input Output types are not matching");
                    }
                    for (int j = 0; j < tokens.length; j++) {
                        if (tokens2[j].equals("datetime")) {
                            tokens2[j] = "string";
                        }
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
    
    public CDAPPipelineInfo generateCDAPPipeline(Map<String, NullableSchema> dataSchema,
            Map<String, CDAPPipelineInfo> wranglerPluginConfigMap) {
        CDAPPipelineInfo pipelineInformation = new CDAPPipelineInfo();
        if (StringUtils.isEmpty(featureSelectionPipeline)) {
            pipelineInformation.setName("Pipeline_" + System.currentTimeMillis());
        } else {
            pipelineInformation.setName(featureSelectionPipeline);
        }
        pipelineInformation.setArtifact(systemArtifact);
        PipelineConfiguration config = generatePipelineConfiguration(dataSchema, wranglerPluginConfigMap,
                featureSelectionPipeline);
        pipelineInformation.setConfig(config);
        LOG.debug("generatedStageMap = \n");
        for (Map.Entry<String, String> entry : generatedStageMap.entrySet()) {
            System.out.println(entry.getKey() + " = " + entry.getValue());
        }
        return pipelineInformation;
    }
    
    private PipelineConfiguration generatePipelineConfiguration(Map<String, NullableSchema> dataSchema,
            Map<String, CDAPPipelineInfo> wranglerPluginConfigMap, String featureSelectionPipeline) {
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
        
        createSourceStages(dataSchema, wranglerPluginConfigMap, pipeLineConfiguration);
        createMissingEntityTables(wranglerPluginConfigMap, this.relationShips, this.entityNames);
        this.targetEntityStageName = lastStageMapForTable.get(this.targetEntity);
        Map<String, String> lastStageMapForTableTillSource = new HashMap<>(lastStageMapForTable);
        List<String> lastStagesForEachTrainingWindow = new LinkedList<String>();
        // List<String> statsStagesForEachTrainingWindow = new LinkedList<String>();
        Map<String, Map<String, List<String>>> originalAppliedAggFunctionsWithArguments = getDeepCopy(
                this.appliedAggFunctionsWithArguments);
        Map<String, Map<String, List<String>>> originalAppliedTransFunctionsWithArguments = getDeepCopy(
                this.appliedTransFunctionsWithArguments);
        
        int processed = 0;
        if (this.trainingWindows != null && !this.trainingWindows.isEmpty()) {
            for (int i = 0; i < this.trainingWindows.size(); i++) {
                Integer trainingTime = this.trainingWindows.get(i);
                if (!dagGeneratorOutputMap.containsKey(trainingTime + "")) {
                    this.trainingWindows.remove(i);
                    i--;
                    continue;
                }
                processed++;
                this.appliedAggFunctionsWithArguments = getDeepCopy(originalAppliedAggFunctionsWithArguments);
                this.appliedTransFunctionsWithArguments = getDeepCopy(originalAppliedTransFunctionsWithArguments);
                CDAPSubDagGeneratorOutput dagGeneratorOutput = dagGeneratorOutputMap.get(trainingTime + "");
                adjustAppliedFunctionWithArgument(appliedAggFunctionsWithArguments,
                        dagGeneratorOutput.getMultiInputColumnSet());
                adjustAppliedFunctionWithArgument(appliedTransFunctionsWithArguments,
                        dagGeneratorOutput.getMultiInputColumnSet());
                featureDagEntryDictionaryMap = dagGeneratorOutput.getFeatureDagEntryDictionaryMap();
                generateCatetgoricalColumnDictionary(dagGeneratorOutput.getCategoricalColumnDictionary());
                categoricalColumnsToBeChecked.clear();
                DateTime windowStartTime = windowEndTime.minusHours(trainingTime);
                this.lastStageMapForTable = new HashMap<>(lastStageMapForTableTillSource);
                for (SchemaColumn timeIndexColumn : timeIndexColumns) {
                    createFilterStage(windowStartTime, windowEndTime, timeIndexColumn);
                }
                populateStagesFromOperations(dagGeneratorOutput.getFeatureSubDag());
                lastStagesForEachTrainingWindow.add(lastStageMapForTable.get(targetEntity));
                // String statsComputeStageName =
                // createStatsComputeStage(lastStageMapForTable.get(targetEntity));
                // statsStagesForEachTrainingWindow.add(statsComputeStageName);
            }
            String sourceTempTableName = takeOuterjoinOfAllTempTables(lastStagesForEachTrainingWindow,
                    targetEntityIdField);
            // String joinedStatsTableName =
            // takeOuterjoinOfAllTempTables(statsStagesForEachTrainingWindow, "Statistic");
            // getElasticSearchSinkNode("StatsElasticsearch", "statsDataSink",
            // "stats_index_" + System.currentTimeMillis(),
            // "stats", "Statistic", joinedStatsTableName);
            String finalMergedTableStageName = takeOuterJoinOfTwoTables(this.targetEntityStageName, sourceTempTableName,
                    this.targetEntityIdField, this.targetEntityIdField, -1);
            lastStageMapForTable.put(targetEntity, finalMergedTableStageName);
            completePipelineAndSerializeIt(pipeLineConfiguration);
            
        } else {
            generatePipelineStagesAndCreateConnections(pipeLineConfiguration,
                    dagGeneratorOutputMap.values().iterator().next().getFeatureSubDag());
        }
        if (processed == 0) {
            generatePipelineStagesAndCreateConnections(pipeLineConfiguration,
                    dagGeneratorOutputMap.values().iterator().next().getFeatureSubDag());
        }
        return pipeLineConfiguration;
    }
    
    private void createMissingEntityTables(Map<String, CDAPPipelineInfo> wranglerPluginConfigMap,
            List<Relation> relationShips, List<String> entityNames) {
        for (SchemaColumn missingTable : this.createEntities) {
            String tableIndex = missingTable.getColumn();
            String entityTable = missingTable.getTable();
            for (Relation relation : relationShips) {
                SchemaColumn col1 = relation.getColumn1();
                SchemaColumn col2 = relation.getColumn2();
                if (col1.getTable().equals(entityTable) && col1.getColumn().equals(tableIndex)) {
                    createSourceDedupedTable(entityTable, tableIndex, relation.getColumn2());
                    break;
                } else if (col2.getTable().equals(entityTable) && col2.getColumn().equals(tableIndex)) {
                    createSourceDedupedTable(entityTable, tableIndex, relation.getColumn1());
                    break;
                }
            }
        }
    }
    
    private void createSourceDedupedTable(String entityTable, String tableIndex, SchemaColumn sourceColumn) {
        String lastStageName = lastStageMapForTable.get(sourceColumn.getTable());
        String currentStageName = entityTable + "_RowDeduper";
        
        PipelineNode pipelineNode = new PipelineNode();
        PluginNode pluginNode = new PluginNode();
        pipelineNode.setPlugin(pluginNode);
        pipelineNode.setName(currentStageName);
        pluginNode.setName("RowDeduper");
        pluginNode.setType("sparkcompute");
        pluginNode.setLabel(currentStageName);
        pluginNode.setArtifact(this.featureEngineeringArtifact);
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("columnToBeDeduped", sourceColumn.getColumn());
        pluginNode.setProperties(properties);
        Schema lastStageSchema = schemaMap.get(sourceColumn.getTable());
        Schema curStageSchema = getColumnSchema(lastStageSchema, sourceColumn.getColumn(), tableIndex);
        pipelineNode.setOutputSchema(GSON_OBJ.toJson(curStageSchema));
        
        List<InOutSchema> inputSchema = new LinkedList<InOutSchema>();
        InOutSchema inSchema = new InOutSchema();
        inputSchema.add(inSchema);
        inSchema.setName(lastStageName);
        inSchema.setSchema(GSON_OBJ.toJson(lastStageSchema));
        pipelineNode.setInputSchema(inputSchema);
        schemaMap.put(currentStageName, curStageSchema);
        stageMap.put(currentStageName, pipelineNode);
        lastStageMapForTable.put(entityTable, currentStageName);
        if (!firstStageMapForTable.containsKey(entityTable)) {
            firstStageMapForTable.put(entityTable, currentStageName);
        }
        putInConnection(lastStageName, currentStageName);
        isUsedStage.add(currentStageName);
        isUsedStage.add(lastStageName);
    }
    
    private Schema getColumnSchema(Schema schema, String sourceColumn, String destinationColumn) {
        Schema resSchema = new Schema();
        resSchema.setName(schema.getName());
        resSchema.setType(schema.getType());
        SchemaFieldName destField = null;
        for (SchemaFieldName field : schema.getFields()) {
            if (field.getName().equals(sourceColumn)) {
                if (field instanceof SchemaField) {
                    SchemaField schemaField = new SchemaField();
                    schemaField.setName(destinationColumn);
                    schemaField.setType(((SchemaField) field).getType());
                    destField = schemaField;
                } else if (field instanceof NullableSchemaField) {
                    NullableSchemaField schemaField = new NullableSchemaField();
                    NullableSchemaField sourceField = (NullableSchemaField) field;
                    schemaField.setName(sourceField.getName());
                    schemaField.setType(sourceField.getType());
                    destField = schemaField;
                }
            }
        }
        SchemaFieldName[] destFields = new SchemaFieldName[1];
        destFields[0] = destField;
        resSchema.setFields(destFields);
        
        return resSchema;
    }
    
    private Map<String, Map<String, List<String>>> getDeepCopy(
            Map<String, Map<String, List<String>>> appliedFunctionsWithArguments) {
        Map<String, Map<String, List<String>>> originalAppliedAggFunctionsWithArguments = new HashMap<>();
        for (Map.Entry<String, Map<String, List<String>>> entry : appliedFunctionsWithArguments.entrySet()) {
            originalAppliedAggFunctionsWithArguments.put(entry.getKey(), new HashMap<>(entry.getValue()));
        }
        return originalAppliedAggFunctionsWithArguments;
    }
    
    private static void adjustAppliedFunctionWithArgument(
            Map<String, Map<String, List<String>>> appliedFunctionsWithArguments, Set<String> multiInputColumnSet) {
        for (Map.Entry<String, Map<String, List<String>>> entry : appliedFunctionsWithArguments.entrySet()) {
            
            entry.getValue().keySet().retainAll(multiInputColumnSet);
        }
    }
    
    private String createStatsComputeStage(String lastStageName) {
        Schema lastStageSchema = schemaMap.get(lastStageName);
        PipelineNode stageNode = new PipelineNode();
        String stageName = lastStageName + "_stats";
        stageNode.setName(stageName);
        PluginNode pluginNode = new PluginNode();
        stageNode.setPlugin(pluginNode);
        pluginNode.setLabel(stageName);
        pluginNode.setName("StatsCompute");
        pluginNode.setType("sparkcompute");
        Artifact esArtifact = new Artifact();
        esArtifact.setName("feature-engineering-plugin");
        esArtifact.setVersion("2.1-1-SNAPSHOT");
        esArtifact.setScope("SYSTEM");
        pluginNode.setArtifact(esArtifact);
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("parallelThreads", "20");
        pluginNode.setProperties(properties);
        
        List<InOutSchema> inputSchema = generateInputSchema(lastStageName, lastStageSchema, stageNode);
        stageNode.setInputSchema(inputSchema);
        Schema outputSchema = generateStatsComputeOutputSchema(lastStageSchema);
        generateOutputSchema("etlSchemaBody", outputSchema, stageNode);
        putInConnection(lastStageName, stageName);
        stageMap.put(stageName, stageNode);
        schemaMap.put(stageName, outputSchema);
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
        if (currentStageTempTableNames.isEmpty()) {
            return null;
        }
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
        } else {
            stageName = destTableLastStage + "_joiner_";
        }
        if (this.generatedStageMap.containsKey(sourceTableLastStage)) {
            stageName += this.generatedStageMap.get(sourceTableLastStage);
        } else {
            stageName += sourceTableLastStage;
        }
        stageName = getNextUniqueIdForStage(stageName);
        stageName = "Joiner_" + stageName;
        PipelineNode pipelineNode = new PipelineNode();
        pipelineNode.setName(stageName);
        PluginNode pluginNode = new PluginNode();
        pipelineNode.setPlugin(pluginNode);
        pluginNode.setArtifact(this.featureEngineeringArtifact);
        pluginNode.setName("JoinerFE");
        pluginNode.setType("batchjoiner");
        pluginNode.setLabel(stageName);
        Map<String, Object> pluginProperties = new HashMap<String, Object>();
        pluginNode.setProperties(pluginProperties);
        Schema currentSchema = new Schema();
        Schema destTableLastStageSchema = schemaMap.get(destTableLastStage);
        Schema sourceTableLastStageSchema = schemaMap.get(sourceTableLastStage);
        Set<String> schemaSet = new HashSet<String>();
        
        List<SchemaFieldName> currentSchemaFieldList = new LinkedList<>();
        currentSchema.setName("etlSchemaBody");
        currentSchema.setType("record");
        int i = 0;
        StringBuilder selectedFields = new StringBuilder();
        for (SchemaFieldName schemaField : destTableLastStageSchema.getFields()) {
            if (schemaField.getName().equals(sourceJoinKey)) {
                continue;
            }
            if (!schemaSet.contains(schemaField.getName())) {
                if (i > 0) {
                    selectedFields.append(",");
                }
                if (index == 1) {
                    selectedFields.append(destTableLastStage + "." + schemaField.getName() + " as "
                            + schemaField.getName() + "_" + this.trainingWindows.get(0));
                    schemaSet.add(schemaField.getName() + "_" + this.trainingWindows.get(0));
                } else {
                    selectedFields
                            .append(destTableLastStage + "." + schemaField.getName() + " as " + schemaField.getName());
                    schemaSet.add(schemaField.getName());
                }
                NullableSchemaField schemaFieldCopy = getNullableSchema(schemaField);
                if (index == 1) {
                    schemaFieldCopy.setName(schemaFieldCopy.getName() + "_" + this.trainingWindows.get(0));
                }
                currentSchemaFieldList.add(schemaFieldCopy);
                i++;
            }
        }
        
        for (SchemaFieldName schemaField : sourceTableLastStageSchema.getFields()) {
            if (!schemaSet.contains(schemaField.getName())) {
                if (i > 0) {
                    selectedFields.append(",");
                }
                if (schemaField.getName().equals(sourceJoinKey)) {
                    selectedFields.append(
                            sourceTableLastStage + "." + schemaField.getName() + " as " + schemaField.getName());
                    currentSchemaFieldList.add(schemaField);
                    schemaSet.add(schemaField.getName());
                } else {
                    if (index >= 0) {
                        selectedFields.append(sourceTableLastStage + "." + schemaField.getName() + " as "
                                + schemaField.getName() + "_" + this.trainingWindows.get(index));
                    } else {
                        selectedFields.append(
                                sourceTableLastStage + "." + schemaField.getName() + " as " + schemaField.getName());
                    }
                    SchemaFieldName schemaFieldCopy = null;
                    if (schemaField instanceof NullableSchemaField) {
                        NullableSchemaField schemaFieldCopy2 = new NullableSchemaField();
                        schemaFieldCopy2.setType(((NullableSchemaField) schemaField).getType());
                        schemaFieldCopy = schemaFieldCopy2;
                    } else if (schemaField instanceof SchemaField) {
                        SchemaField schemaFieldCopy2 = new SchemaField();
                        schemaFieldCopy2.setType(((SchemaField) schemaField).getType());
                        schemaFieldCopy = schemaFieldCopy2;
                    }
                    if (index >= 0) {
                        schemaSet.add(schemaField.getName() + "_" + this.trainingWindows.get(index));
                        schemaFieldCopy.setName(schemaField.getName() + "_" + this.trainingWindows.get(index));
                    } else {
                        schemaSet.add(schemaField.getName());
                        schemaFieldCopy.setName(schemaField.getName());
                    }
                    currentSchemaFieldList.add(schemaFieldCopy);
                }
                i++;
            }
        }
        
        currentSchema.setFields(currentSchemaFieldList.toArray(new SchemaFieldName[0]));
        pluginProperties.put("joinKeys",
                sourceTableLastStage + "." + sourceJoinKey + " = " + destTableLastStage + "." + destJoinKey);
        pluginProperties.put("requiredInputs", sourceTableLastStage);
        pluginProperties.put("selectedFields", selectedFields.toString());
        List<InOutSchema> inputSchemaList = generateInputSchema(sourceTableLastStage, sourceTableLastStageSchema,
                pipelineNode);
        inputSchemaList.addAll(generateInputSchema(destTableLastStage, destTableLastStageSchema, pipelineNode));
        pipelineNode.setInputSchema(inputSchemaList);
        generateOutputSchema("etlSchemaBody", currentSchema, pipelineNode);
        schemaMap.put(stageName, currentSchema);
        stageMap.put(stageName, pipelineNode);
        putInConnection(sourceTableLastStage, stageName);
        putInConnection(destTableLastStage, stageName);
        isUsedStage.add(sourceTableLastStage);
        isUsedStage.add(stageName);
        isUsedStage.add(destTableLastStage);
        return stageName;
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
        getCDAPTableSinkNode("FeatureValues", lastStageMapForTable.get(targetEntity), targetEntityIdField);
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
    
    private PipelineNode getCDAPTableSinkNode(final String stageName, final String lastStageName,
            final String targetEntityIdField) {
        PipelineNode stageNode = new PipelineNode();
        stageNode.setName(stageName);
        PluginNode pluginNode = new PluginNode();
        stageNode.setPlugin(pluginNode);
        pluginNode.setLabel(stageName);
        pluginNode.setName("Table");
        pluginNode.setType("batchsink");
        pluginNode.setArtifact(this.pluginArtifact);
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("name", featureSelectionPipeline);
        properties.put("schema.row.field", targetEntityIdField); // Test or make changes to make sure
                                                                 // targetEntityIdField is coming in schema
                                                                 // specifically with multiple time windows.
        properties.put("schema", GSON_OBJ.toJson(getNullableSchema(schemaMap.get(lastStageName))));
        pluginNode.setProperties(properties);
        
        putInConnection(lastStageName, stageName);
        stageMap.put(stageName, stageNode);
        isUsedStage.add(lastStageName);
        isUsedStage.add(stageName);
        return stageNode;
    }
    
    private Schema getNullableSchema(Schema schema) {
        Schema resultSchema = new Schema();
        resultSchema.setName(schema.getName());
        resultSchema.setType(schema.getType());
        SchemaFieldName[] fields = new SchemaFieldName[schema.getFields().length];
        int index = 0;
        for (SchemaFieldName field : schema.getFields()) {
            fields[index++] = getNullableSchema(field);
        }
        resultSchema.setFields(fields);
        return resultSchema;
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
            if (columnDataTypeMap.isEmpty()) {
                continue;
            }
            Map<String, Map<String, List<String>>> pluginColumnDataTypeMap = new HashMap<>();
            Map<String, PluginSummary> pluginSummaryMap = new HashMap<>();
            getPluginColumnDataTyepMap(columnDataTypeMap, pluginColumnDataTypeMap, pluginSummaryMap,
                    this.multiInputTransformPluginFunctionMap);
            
            for (Map.Entry<String, Map<String, List<String>>> entry2 : pluginColumnDataTypeMap.entrySet()) {
                String lastStageName = this.lastStageMapForTable.get(tableName);
                Schema lastStageSchema = schemaMap.get(lastStageName);
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
                pluginNode.setArtifact(featureEngineeringArtifact);
                Map<String, Object> properties = new HashMap<String, Object>();
                StringBuilder sb = new StringBuilder();
                List<SchemaFieldName> currentSchemaFieldList = new LinkedList<>();
                for (SchemaFieldName fieldName : lastStageSchema.getFields()) {
                    currentSchemaFieldList.add(fieldName);
                }
                SchemaField currentSchemaField = new SchemaField();
                int index = 0;
                for (Map.Entry<String, List<String>> entry3 : entry2.getValue().entrySet()) {
                    if (index > 0) {
                        sb.append(",");
                    }
                    sb.append(entry3.getKey() + ":" + entry3.getValue().get(0).trim().toUpperCase() + "(");
                    sb.append(entry3.getValue().get(1).trim());
                    sb.append(")");
                    index++;
                    currentSchemaField = new SchemaField();
                    currentSchemaField.setName(entry3.getKey());
                    currentSchemaField.setType(entry3.getValue().get(2));
                    currentSchemaFieldList.add(currentSchemaField);
                }
                properties.put("primitives", sb.toString());
                
                pluginNode.setProperties(properties);
                Schema currentSchema = new Schema();
                currentSchema.setName(lastStageSchema.getName());
                currentSchema.setType(lastStageSchema.getType());
                
                currentSchema.setFields(currentSchemaFieldList.toArray(new SchemaFieldName[0]));
                setInputOutputSchema(lastStageSchema, currentSchema, lastStageMapForTable.get(tableName), pipelineNode);
                putInConnection(lastStageMapForTable.get(tableName), currentStageName);
                isUsedStage.add(lastStageMapForTable.get(tableName));
                isUsedStage.add(currentStageName);
                schemaMap.put(currentStageName, currentSchema);
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
        String tableName = timeIndexColumn.getTable();
        String columnName = timeIndexColumn.getColumn();
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
        pluginNode.setArtifact(featureEngineeringArtifact);
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("filters", windowEndTime.getMillis() + ":LTE(" + columnName + ")," + windowStartTime.getMillis()
                + ":GTE(" + columnName + ")");
        pluginNode.setProperties(properties);
        Schema lastStageSchema = schemaMap.get(lastStageName);
        
        pipelineNode.setOutputSchema(GSON_OBJ.toJson(lastStageSchema));
        
        List<InOutSchema> inputSchema = new LinkedList<InOutSchema>();
        InOutSchema inSchema = new InOutSchema();
        inputSchema.add(inSchema);
        inSchema.setName(lastStageName);
        inSchema.setSchema(GSON_OBJ.toJson(lastStageSchema));
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
            if (this.createEntitiesMap.containsKey(tableName)) {
                continue;
            }
            for (BasePipelineNode stage : wranglerPluginEntry.getValue().getConfig().getStages()) {
                String stageName = tableName + "_" + stage.getName();
                stage.setName(stageName);
                stage.getPlugin().setLabel(stageName);
                stageMap.put(stageName, stage);
            }
            schemaMap.put(tableName, getSchemaObjectFromNullable(inputDataSourceInfoMap.get(tableName)));
            List<Connection> connections = wranglerPluginEntry.getValue().getConfig().getConnections();
            if (connections != null && !connections.isEmpty()) {
                for (Connection connection : connections) {
                    String sourceConnectionName = tableName + "_" + connection.from;
                    String destinationConnectionName = tableName + "_" + connection.to;
                    putInConnection(sourceConnectionName, destinationConnectionName);
                    lastStageMapForTable.put(tableName, destinationConnectionName);
                    if (!firstStageMapForTable.containsKey(tableName)) {
                        firstStageMapForTable.put(tableName, sourceConnectionName);
                    }
                    schemaMap.put(destinationConnectionName, schemaMap.get(tableName));
                    isUsedStage.add(sourceConnectionName);
                    isUsedStage.add(destinationConnectionName);
                }
            } else {
                for (BasePipelineNode stage : wranglerPluginEntry.getValue().getConfig().getStages()) {
                    String stageName = tableName + "_" + stage.getName();
                    lastStageMapForTable.put(tableName, stageName);
                    if (!firstStageMapForTable.containsKey(tableName)) {
                        firstStageMapForTable.put(tableName, stageName);
                    }
                }
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
    
    private void createSourceStages(Map<String, Map<String, Object>> inputDataSourceInfoMap) {
        for (Map.Entry<String, Map<String, Object>> entry : inputDataSourceInfoMap.entrySet()) {
            String tableName = entry.getKey();
            Schema schema = new Schema();
            StagePipelineNode stageNode = getSourceStagePipelineNode(entry.getValue(), schema);
            stageMap.put(tableName, stageNode);
            schemaMap.put(tableName, (Schema) entry.getValue().get("schema"));
            Schema inputSchema = (Schema) entry.getValue().get("schema");
            String csvStageName = "CSVParser_" + tableName;
            PipelineNode csvParserStageNode = getCSVParserStagePipelineNode(entry.getValue(), csvStageName, schema);
            
            stageMap.put(csvStageName, csvParserStageNode);
            schemaMap.put(csvStageName, inputSchema);
            lastStageMapForTable.put(tableName, csvStageName);
            putInConnection(tableName, csvStageName);
            isUsedStage.add(tableName);
            isUsedStage.add(csvStageName);
        }
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
        properties.put("schema", GSON_OBJ.toJson((Schema) dataSourceInfo.get("schema")));
        properties.put("field", "body");
        pluginNode.setProperties(properties);
        
        pipelineNode.setOutputSchema(GSON_OBJ.toJson((Schema) dataSourceInfo.get("schema")));
        
        List<InOutSchema> inputSchema = new LinkedList<InOutSchema>();
        InOutSchema inSchema = new InOutSchema();
        inputSchema.add(inSchema);
        inSchema.setName((String) dataSourceInfo.get("name"));
        inSchema.setSchema(GSON_OBJ.toJson(schema));
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
        properties.put("schema", GSON_OBJ.toJson(schema));
        pluginNode.setProperties(properties);
        pipelineNode.setOutputSchema(GSON_OBJ.toJson(schema));
        return pipelineNode;
    }
    
    private PipelineNode getElasticSearchSinkNode(String stageName, String referenceName, String esIndex, String esType,
            String esIdField, String lastStageName) {
        Schema lastStageSchema = schemaMap.get(lastStageName);
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
        
        List<InOutSchema> inputSchema = generateInputSchema(lastStageName, lastStageSchema, stageNode);
        stageNode.setInputSchema(inputSchema);
        generateOutputSchema("etlSchemaBody", lastStageSchema, stageNode);
        putInConnection(lastStageName, stageName);
        stageMap.put(stageName, stageNode);
        schemaMap.put(stageName, lastStageSchema);
        isUsedStage.add(lastStageName);
        isUsedStage.add(stageName);
        return stageNode;
    }
    
    private void truncateIslandNodesFromDAG() {
        for (String entity : entityNames) {
            if (entity.equals(targetEntity)) {
                continue;
            }
            entity = firstStageMapForTable.get(entity);
            if (isConnectedToReachableNodes(entity)) {
                continue;
            } else {
                deleteGraphFromEntityNode(entity);
            }
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
        if (isNodeReachable.contains(entity)) {
            return true;
        }
        Set<String> destinations = connections.get(entity);
        if (destinations != null) {
            for (String dest : destinations) {
                if (isNodeReachable.contains(dest)) {
                    return true;
                }
                boolean result = isConnectedToReachableNodes(dest);
                if (result) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private void markReachableNodesFromTargetEntity(String root) {
        if (!connections.containsKey(root) && !connections.containsKey(firstStageMapForTable.get(root))) {
            return;
        }
        isNodeReachable.add(root);
        Set<String> destinations = connections.get(root);
        if (destinations != null) {
            for (String dest : destinations) {
                if (isNodeReachable.contains(dest)) {
                    continue;
                }
                isNodeReachable.add(dest);
                markReachableNodesFromTargetEntity(dest);
            }
        }
    }
    
    private void generateTrashSinkStage() {
        Set<String> toBeTrashedStages = new HashSet<String>(stageMap.keySet());
        for (String stageName : toBeTrashedStages) {
            if (connections.containsKey(stageName) || isStageSinkType(stageName)) {
                continue;
            }
            addTrashSinkForStage(stageName);
        }
    }
    
    private boolean isStageSinkType(String stageName) {
        BasePipelineNode node = stageMap.get(stageName);
        if (node.getPlugin().getType().equals("batchsink")) {
            return true;
        }
        return false;
    }
    
    private PipelineNode addTrashSinkForStage(String lastStageName) {
        Schema lastStageSchema = schemaMap.get(lastStageName);
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
        
        List<InOutSchema> inputSchema = generateInputSchema(lastStageName, lastStageSchema, trashStage);
        trashStage.setInputSchema(inputSchema);
        generateOutputSchema("etlSchemaBody", lastStageSchema, trashStage);
        putInConnection(lastStageName, currentStageName);
        stageMap.put(currentStageName, trashStage);
        schemaMap.put(currentStageName, lastStageSchema);
        isUsedStage.add(lastStageName);
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
            return;
            // throw new IllegalStateException("Input Operation Data is not complete");
        }
        String tokens2[] = tokens[1].substring(1, tokens[1].length() - 1).split("::");
        String sourceJoinKey = "";
        String destJoinKey = "";
        for (int i = 0; i < 2; i++) {
            String token3[] = tokens2[i].split("\\.");
            if (token3[0].equals(sourceTable)) {
                sourceJoinKey = token3[1];
            } else {
                destJoinKey = token3[1];
            }
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
        if (currentStageTempTableNames.isEmpty()) {
            return null;
        }
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
        String stageName = "";
        if (this.generatedStageMap.containsKey(destTableLastStage)) {
            stageName = this.generatedStageMap.get(destTableLastStage) + "_joiner_";
        } else {
            stageName = destTableLastStage + "_joiner_";
        }
        if (this.generatedStageMap.containsKey(sourceTableLastStage)) {
            stageName += this.generatedStageMap.get(sourceTableLastStage);
        } else {
            stageName += sourceTableLastStage;
        }
        stageName = getNextUniqueIdForStage(stageName);
        stageName = "Joiner_" + stageName;
        PipelineNode pipelineNode = new PipelineNode();
        pipelineNode.setName(stageName);
        PluginNode pluginNode = new PluginNode();
        pipelineNode.setPlugin(pluginNode);
        pluginNode.setArtifact(this.featureEngineeringArtifact);
        pluginNode.setName("JoinerFE");
        pluginNode.setType("batchjoiner");
        pluginNode.setLabel(stageName);
        Map<String, Object> pluginProperties = new HashMap<String, Object>();
        pluginNode.setProperties(pluginProperties);
        Schema currentSchema = new Schema();
        Schema destTableLastStageSchema = schemaMap.get(destTableLastStage);
        Schema sourceTableLastStageSchema = schemaMap.get(sourceTableLastStage);
        Set<String> schemaSet = new HashSet<String>();
        
        List<SchemaFieldName> currentSchemaFieldList = new LinkedList<>();
        currentSchema.setName("etlSchemaBody");
        currentSchema.setType("record");
        int i = 0;
        StringBuilder selectedFields = new StringBuilder();
        for (SchemaFieldName schemaField : destTableLastStageSchema.getFields()) {
            if (i > 0) {
                selectedFields.append(",");
            }
            boolean added = schemaSet.add(schemaField.getName());
            if (added) {
                selectedFields
                        .append(destTableLastStage + "." + schemaField.getName() + " as " + schemaField.getName());
                currentSchemaFieldList.add(schemaField);
                i++;
            }
        }
        
        for (SchemaFieldName schemaField : sourceTableLastStageSchema.getFields()) {
            if (schemaField.getName().equals(sourceJoinKey)) {
                continue;
            }
            if (i > 0) {
                selectedFields.append(",");
            }
            boolean added = schemaSet.add(schemaField.getName());
            if (added) {
                selectedFields
                        .append(sourceTableLastStage + "." + schemaField.getName() + " as " + schemaField.getName());
                currentSchemaFieldList.add(schemaField);
                i++;
            }
        }
        currentSchema.setFields(currentSchemaFieldList.toArray(new SchemaFieldName[0]));
        pluginProperties.put("joinKeys",
                sourceTableLastStage + "." + sourceJoinKey + " = " + destTableLastStage + "." + destJoinKey);
        pluginProperties.put("requiredInputs", sourceTableLastStage + ", " + destTableLastStage);
        pluginProperties.put("selectedFields", selectedFields.toString());
        if (enablePartitions) {
            pluginProperties.put("numPartitions", NUM_PARTITIONS);
        }
        List<InOutSchema> inputSchemaList = generateInputSchema(sourceTableLastStage, sourceTableLastStageSchema,
                pipelineNode);
        inputSchemaList.addAll(generateInputSchema(destTableLastStage, destTableLastStageSchema, pipelineNode));
        pipelineNode.setInputSchema(inputSchemaList);
        generateOutputSchema("etlSchemaBody", currentSchema, pipelineNode);
        schemaMap.put(stageName, currentSchema);
        stageMap.put(stageName, pipelineNode);
        putInConnection(sourceTableLastStage, stageName);
        putInConnection(destTableLastStage, stageName);
        isUsedStage.add(sourceTableLastStage);
        isUsedStage.add(stageName);
        isUsedStage.add(destTableLastStage);
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
        } else {
            stageName = sourceTableLastStage;
        }
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
        Schema currentSchema = new Schema();
        Schema lastSchema = schemaMap.get(sourceTableLastStage);
        if (pluginSummary.getName().equals("GroupByAggregateFE")) {
            createGroupByAggregateStage(pipelineNode, pluginNode, currentSchema, lastSchema, columnOperations,
                    sourceJoinKey, pluginProperties, pluginSummary, sourceTableLastStage);
        } else if (pluginSummary.getName().equals("GroupByCategoricalAggregate")) {
            createGroupByCategoricalAggregateStage(pipelineNode, pluginNode, currentSchema, lastSchema,
                    columnOperations, sourceJoinKey, pluginProperties, pluginSummary, sourceTableLastStage,
                    sourceTable);
        }
        schemaMap.put(stageName, currentSchema);
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
            if (columnDataTypeMap.isEmpty()) {
                continue;
            }
            String sourceJoinKey = "";
            String destTable = "";
            String destJoinKey = "";
            String lastStageName = this.lastStageMapForTable.get(tableName);
            Map<String, Map<String, List<String>>> pluginColumnDataTypeMap = new HashMap<>();
            Map<String, PluginSummary> pluginSummaryMap = new HashMap<>();
            getPluginColumnDataTyepMap(columnDataTypeMap, pluginColumnDataTypeMap, pluginSummaryMap,
                    this.multiInputAggregatePluginFunctionMap);
            List<String> currentStageMultiInputGroupByTempTableNames = new LinkedList<String>();
            String index = this.tableIndexMap.get(tableName);
            for (Map.Entry<String, Map<String, List<String>>> entry2 : pluginColumnDataTypeMap.entrySet()) {
                PluginSummary pluginSummary = pluginSummaryMap.get(entry2.getKey());
                PipelineNode pipelineNode = new PipelineNode();
                String currentStageName = "";
                if (this.generatedStageMap.containsKey(lastStageName)) {
                    currentStageName = this.generatedStageMap.get(lastStageName);
                } else {
                    currentStageName = lastStageName;
                }
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
                Schema currentSchema = new Schema();
                Schema lastSchema = schemaMap.get(lastStageName);
                if (pluginSummary.getName().equals("GroupByCategoricalAggregate")) {
                    createGroupByMultiInputCategoricalAggregateStage(pipelineNode, pluginNode, currentSchema,
                            lastSchema, entry2.getValue(), pluginProperties, pluginSummary, lastStageName, tableName);
                }
                schemaMap.put(currentStageName, currentSchema);
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
            Schema currentSchema, Schema lastSchema, Map<String, List<String>> toBeComputedColumnTypeMap,
            Map<String, Object> pluginProperties, PluginSummary pluginSummary, String lastStageName, String tableName) {
        
        currentSchema.setName(lastSchema.getName());
        currentSchema.setType(lastSchema.getType());
        List<SchemaFieldName> currentSchemaFieldList = new LinkedList<>();
        String sourceJoinKey = toBeComputedColumnTypeMap.values().iterator().next().get(3);
        if (sourceJoinKey == null) {
            throw new IllegalStateException("Group By Key not found in table " + tableName
                    + " for createGroupByMultiInputCategoricalAggregateStage operation");
        }
        SchemaField currentSchemaField = new SchemaField();
        String sourceJoinKeyType = "";
        Map<String, String> currentSchemaMap = new HashMap<String, String>();
        for (SchemaFieldName field : lastSchema.getFields()) {
            currentSchemaMap.put(field.getName(), getSchemaType(field));
            if (field.getName().equals(sourceJoinKey)) {
                sourceJoinKeyType = getSchemaType(field);
            }
        }
        
        StringBuilder aggregates = new StringBuilder();
        Map<String, Set<String>> categoricalDictionaryMap = new HashMap<>();
        StringBuilder categoricalDictionary = new StringBuilder();
        int categoricalDictionaryIndex = 0;
        int index = 0;
        currentSchemaField = new SchemaField();
        currentSchemaField.setName(sourceJoinKey);
        if (sourceJoinKeyType.equals("")) {
            throw new IllegalStateException(
                    "Group By joinkey is missing in input schema " + sourceJoinKey + " in schema map=");
        }
        currentSchemaField.setType(sourceJoinKeyType);
        currentSchemaFieldList.add(currentSchemaField);
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
            
            Set<String> dictionary = getCrossProductDictionaryFromMultipleColumns(token[0].toLowerCase(), tableName,
                    categoricalDictionaryMap);
            
            if (dictionary == null) {
                throw new IllegalStateException(
                        "Dictionary is not populated for column " + tableName + "." + token[0].toLowerCase());
            }
            LOG.debug("Original Dictionary for " + token[1] + " is dictionary=" + dictionary);
            Set<String> selectedDictionary = this.featureDagEntryDictionaryMap
                    .get(trimByChar(token[1].toLowerCase(), '_'));
            if (selectedDictionary == null) {
                dictionary.clear();
            } else {
                dictionary.retainAll(selectedDictionary);
            }
            LOG.debug("Modified Dictionary for " + token[1] + " is dictionary=" + dictionary);
            categoricalColumnsToBeChecked.put(token[1].toLowerCase(), dictionary);
            if (categoricalDictionaryIndex > 0) {
                categoricalDictionary.append(",");
                categoricalDictionaryIndex = 0;
            }
            categoricalDictionary.append(token[1].toLowerCase() + ":");
            for (String dict : dictionary) {
                currentSchemaField = new SchemaField();
                currentSchemaField.setName(token[1].toLowerCase() + "_" + dict.toLowerCase());
                currentSchemaField.setType(outputType);
                currentSchemaFieldList.add(currentSchemaField);
                
                if (categoricalDictionaryIndex > 0) {
                    categoricalDictionary.append(";");
                }
                categoricalDictionary.append(dict.toLowerCase());
                categoricalDictionaryIndex++;
            }
            index++;
        }
        
        currentSchema.setFields(currentSchemaFieldList.toArray(new SchemaFieldName[0]));
        pluginProperties.put("aggregates", aggregates.toString());
        if (enablePartitions) {
            pluginProperties.put("numPartitions", NUM_PARTITIONS);
        }
        pluginProperties.put("groupByFields", sourceJoinKey);
        pluginProperties.put("categoricalDictionary", categoricalDictionary.toString());
        generateOutputSchema("etlSchemaBody", currentSchema, pipelineNode);
        List<InOutSchema> inputSchemaList = generateInputSchema(lastStageName, schemaMap.get(lastStageName),
                pipelineNode);
        pipelineNode.setInputSchema(inputSchemaList);
    }
    
    private Set<String> getCrossProductDictionaryFromMultipleColumns(String columnString, String tableName,
            Map<String, Set<String>> categoricalSingleDictionaryMap) {
        Set<String> dictionary = new HashSet<String>();
        String[] token = columnString.trim().split(" ");
        if (token.length > 2) {
            throw new IllegalStateException("More than 2 combination of categorical dictionary is not supported");
        }
        Map<String, Set<String>> columnDictionaryMap = categoricalColumnDictionaryMap.get(tableName);
        if (columnDictionaryMap == null) {
            return dictionary;
        }
        Set<String> dictionary1 = columnDictionaryMap.get(token[0].toLowerCase());
        Set<String> dictionary2 = columnDictionaryMap.get(token[1].toLowerCase());
        categoricalSingleDictionaryMap.put(token[0].toLowerCase(), dictionary1);
        categoricalSingleDictionaryMap.put(token[1].toLowerCase(), dictionary2);
        for (String dict2 : dictionary2) {
            for (String dict1 : dictionary1) {
                dictionary.add(dict1 + "__" + dict2);
            }
        }
        return dictionary;
    }
    
    private void createGroupByCategoricalAggregateStage(PipelineNode pipelineNode, PluginNode pluginNode,
            Schema currentSchema, Schema lastSchema, List<String> columnOperations, String sourceJoinKey,
            Map<String, Object> pluginProperties, PluginSummary pluginSummary, String sourceTableLastStage,
            String sourceTable) {
        currentSchema.setName(lastSchema.getName());
        currentSchema.setType(lastSchema.getType());
        List<SchemaFieldName> currentSchemaFieldList = new LinkedList<>();
        
        SchemaField currentSchemaField = new SchemaField();
        Map<String, String> currentSchemaMap = new HashMap<String, String>();
        String sourceJoinKeyType = "";
        for (SchemaFieldName field : lastSchema.getFields()) {
            currentSchemaMap.put(field.getName(), getSchemaType(field));
            if (field.getName().equals(sourceJoinKey)) {
                sourceJoinKeyType = getSchemaType(field);
            }
        }
        
        StringBuilder aggregates = new StringBuilder();
        StringBuilder categoricalDictionary = new StringBuilder();
        int categoricalDictionaryIndex = 0;
        int index = 0;
        currentSchemaField = new SchemaField();
        currentSchemaField.setName(sourceJoinKey);
        if (sourceJoinKeyType.equals("")) {
            throw new IllegalStateException("Group By joinkey is missing in input schema " + sourceJoinKey
                    + " in schema map=" + currentSchemaMap);
        }
        currentSchemaField.setType(sourceJoinKeyType);
        currentSchemaFieldList.add(currentSchemaField);
        for (String columnOperation : columnOperations) {
            String token[] = columnOperation.split(",");
            if (index > 0) {
                aggregates.append(",");
            }
            aggregates.append(token[1] + ":" + token[2].toUpperCase() + "(" + token[0] + ")");
            if (!currentSchemaMap.containsKey(token[0])) {
                throw new IllegalStateException("Input Column is not present in last stage schema");
            }
            String outputType = getOutputType(currentSchemaMap.get(token[0]), token[2], pluginSummary.getName(),
                    functionDataTypeInfoMapAggregate);
            if (outputType.startsWith("list")) {
                outputType = getOutputTypeFromListType(outputType);
                Map<String, Set<String>> columnDictionaryMap = categoricalColumnDictionaryMap.get(sourceTable);
                if (columnDictionaryMap == null) {
                    continue;
                }
                Set<String> dictionary = columnDictionaryMap.get(token[0].toLowerCase());
                if (dictionary == null) {
                    throw new IllegalStateException(
                            "Dictionary is not populated for column " + sourceTable + "." + token[0].toLowerCase());
                }
                dictionary = new HashSet<String>(dictionary);
                LOG.debug("Original Dictionary for " + token[1] + " is dictionary=" + dictionary);
                Set<String> selectedDictionarySet = this.featureDagEntryDictionaryMap
                        .get(trimByChar(token[1].toLowerCase(), '_'));
                if (selectedDictionarySet == null) {
                    dictionary.clear();
                } else {
                    dictionary.retainAll(selectedDictionarySet);
                }
                LOG.debug("Modified Dictionary for " + token[1] + " is dictionary=" + dictionary);
                categoricalColumnsToBeChecked.put(token[1].toLowerCase(), dictionary);
                
                if (categoricalDictionaryIndex > 0) {
                    categoricalDictionary.append(",");
                    categoricalDictionaryIndex = 0;
                }
                categoricalDictionary.append(token[1].toLowerCase() + ":");
                for (String dict : dictionary) {
                    currentSchemaField = new SchemaField();
                    currentSchemaField.setName(token[1].toLowerCase() + "_" + dict.toLowerCase());
                    currentSchemaField.setType(outputType);
                    currentSchemaFieldList.add(currentSchemaField);
                    if (categoricalDictionaryIndex > 0) {
                        categoricalDictionary.append(";");
                    }
                    categoricalDictionary.append(dict.toLowerCase());
                    categoricalDictionaryIndex++;
                }
                index++;
                continue;
            }
            currentSchemaField = new SchemaField();
            currentSchemaField.setName(token[1]);
            currentSchemaField.setType(outputType);
            currentSchemaFieldList.add(currentSchemaField);
            index++;
        }
        currentSchema.setFields(currentSchemaFieldList.toArray(new SchemaFieldName[0]));
        pluginProperties.put("aggregates", aggregates.toString());
        if (enablePartitions) {
            pluginProperties.put("numPartitions", NUM_PARTITIONS);
        }
        pluginProperties.put("groupByFields", sourceJoinKey);
        pluginProperties.put("categoricalDictionary", categoricalDictionary.toString());
        generateOutputSchema("etlSchemaBody", currentSchema, pipelineNode);
        List<InOutSchema> inputSchemaList = generateInputSchema(sourceTableLastStage,
                schemaMap.get(sourceTableLastStage), pipelineNode);
        pipelineNode.setInputSchema(inputSchemaList);
    }
    
    private String trimByChar(String input, char trimChar) {
        int st = 0, end = input.length() - 1;
        while (st < input.length() && input.charAt(st) == trimChar) {
            st++;
        }
        
        while (end >= 0 && input.charAt(end) == trimChar) {
            end--;
        }
        if (st > end) {
            return "";
        }
        return input.substring(st, end + 1);
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
    
    private void createGroupByAggregateStage(PipelineNode pipelineNode, PluginNode pluginNode, Schema currentSchema,
            Schema lastSchema, List<String> columnOperations, String sourceJoinKey,
            Map<String, Object> pluginProperties, PluginSummary pluginSummary, String sourceTableLastStage) {
        
        currentSchema.setName(lastSchema.getName());
        currentSchema.setType(lastSchema.getType());
        SchemaField currentSchemaField = new SchemaField();
        List<SchemaFieldName> currentSchemaFieldList = new LinkedList<>();
        Map<String, String> currentSchemaMap = new HashMap<String, String>();
        String sourceJoinKeyType = "";
        for (SchemaFieldName field : lastSchema.getFields()) {
            currentSchemaMap.put(field.getName(), getSchemaType(field));
            if (field.getName().equals(sourceJoinKey)) {
                sourceJoinKeyType = getSchemaType(field);
            }
        }
        
        StringBuilder aggregates = new StringBuilder();
        int index = 0;
        currentSchemaField = new SchemaField();
        currentSchemaField.setName(sourceJoinKey);
        if (sourceJoinKeyType.equals("")) {
            throw new IllegalStateException("Group By joinkey is missing in input schema " + sourceJoinKey
                    + " in schema map=" + currentSchemaMap);
        }
        currentSchemaField.setType(sourceJoinKeyType);
        currentSchemaFieldList.add(currentSchemaField);
        for (String columnOperation : columnOperations) {
            String token[] = columnOperation.split(",");
            boolean added = false;
            for (Map.Entry<String, Set<String>> entry : categoricalColumnsToBeChecked.entrySet()) {
                String dictColumn = entry.getKey();
                if (token[0].contains(dictColumn)) {
                    List<SchemaFieldName> matchingFields = getAllMatchingSchemaFields(lastSchema.getFields(), token[0]);
                    for (SchemaFieldName field : matchingFields) {
                        String alias = token[2].toLowerCase() + "_" + sourceTableLastStage + "_" + field.getName()
                                + "_";
                        if (!aliasIsInFeatureDagEntryMap(alias)) {
                            continue;
                        }
                        if (index > 0) {
                            aggregates.append(",");
                        }
                        String outputType = getOutputType(currentSchemaMap.get(field.getName()), token[2],
                                pluginSummary.getName(), functionDataTypeInfoMapAggregate);
                        aggregates.append(alias + ":" + token[2].toUpperCase() + "(" + field.getName() + ")");
                        currentSchemaField = new SchemaField();
                        currentSchemaField.setName(alias);
                        currentSchemaField.setType(outputType);
                        currentSchemaFieldList.add(currentSchemaField);
                        added = true;
                        index++;
                    }
                }
            }
            if (!added) {
                if (index > 0) {
                    aggregates.append(",");
                }
                String outputType = getOutputType(currentSchemaMap.get(token[0]), token[2], pluginSummary.getName(),
                        functionDataTypeInfoMapAggregate);
                aggregates.append(token[1] + ":" + token[2].toUpperCase() + "(" + token[0] + ")");
                if (!currentSchemaMap.containsKey(token[0])) {
                    throw new IllegalStateException("Input Column is not present in last stage schema");
                }
                currentSchemaField = new SchemaField();
                currentSchemaField.setName(token[1]);
                currentSchemaField.setType(outputType);
                currentSchemaFieldList.add(currentSchemaField);
                index++;
            }
        }
        currentSchema.setFields(currentSchemaFieldList.toArray(new SchemaFieldName[0]));
        pluginProperties.put("aggregates", aggregates.toString());
        pluginProperties.put("groupByFields", sourceJoinKey);
        if (enablePartitions) {
            pluginProperties.put("numPartitions", NUM_PARTITIONS);
        }
        generateOutputSchema("etlSchemaBody", currentSchema, pipelineNode);
        List<InOutSchema> inputSchemaList = generateInputSchema(sourceTableLastStage,
                schemaMap.get(sourceTableLastStage), pipelineNode);
        pipelineNode.setInputSchema(inputSchemaList);
    }
    
    private boolean aliasIsInFeatureDagEntryMap(String alias) {
        for (Map.Entry<String, Set<String>> entry : this.featureDagEntryDictionaryMap.entrySet()) {
            if (!alias.startsWith(entry.getKey())) {
                continue;
            }
            for (String dict : entry.getValue()) {
                if (alias.contains(dict)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private void generateJoinerForTransformation(String sourceTableLastStage, String destTableLastStage,
            String sourceJoinKey, String destJoinKey, String[] tokens, String destTable, String sourceTable) {
        
        PipelineNode pipelineNode = new PipelineNode();
        // String stageName = destTableLastStage + "_joiner_" + sourceTableLastStage;
        String stageName = "";
        if (this.generatedStageMap.containsKey(destTableLastStage)) {
            stageName = this.generatedStageMap.get(destTableLastStage) + "_joiner_";
        } else {
            stageName = destTableLastStage + "_joiner_";
        }
        if (this.generatedStageMap.containsKey(sourceTableLastStage)) {
            stageName += this.generatedStageMap.get(sourceTableLastStage);
        } else {
            stageName += sourceTableLastStage;
        }
        stageName = getNextUniqueIdForStage(stageName);
        stageName = "Joiner_" + stageName;
        pipelineNode.setName(stageName);
        PluginNode pluginNode = new PluginNode();
        pipelineNode.setPlugin(pluginNode);
        pluginNode.setArtifact(this.featureEngineeringArtifact);
        pluginNode.setName("JoinerFE");
        pluginNode.setType("batchjoiner");
        pluginNode.setLabel(stageName);
        Map<String, Object> pluginProperties = new HashMap<String, Object>();
        pluginNode.setProperties(pluginProperties);
        Schema currentSchema = new Schema();
        Schema destTableLastStageSchema = schemaMap.get(destTableLastStage);
        Schema sourceTableLastStageSchema = schemaMap.get(sourceTableLastStage);
        List<SchemaFieldName> currentSchemaFieldList = new LinkedList<>();
        SchemaField currentSchemaFields = new SchemaField();
        currentSchema.setName("etlSchemaBody");
        currentSchema.setType("record");
        int index = 0;
        StringBuilder selectedFields = new StringBuilder();
        Set<String> schemaSet = new HashSet<>();
        for (SchemaFieldName schemaField : destTableLastStageSchema.getFields()) {
            if (index > 0) {
                selectedFields.append(",");
            }
            selectedFields.append(destTableLastStage + "." + schemaField.getName() + " as " + schemaField.getName());
            currentSchemaFieldList.add(schemaField);
            schemaSet.add(schemaField.getName());
            index++;
        }
        Map<String, String> sourceDataTypeMap = getDataTypeMap(sourceTableLastStageSchema.getFields());
        for (int i = 2; i < tokens.length; i++) {
            String tokens2[] = tokens[i].substring(1, tokens[i].length() - 1).trim().split(",");
            if (tokens2.length != 2) {
                throw new IllegalStateException("Input operations for direct relation has wrong input " + tokens[i]);
            }
            
            boolean added = false;
            for (Map.Entry<String, Set<String>> entry : categoricalColumnsToBeChecked.entrySet()) {
                String dictColumn = entry.getKey();
                if (tokens2[0].contains(dictColumn)) {
                    List<SchemaFieldName> matchingFields = getAllMatchingSchemaFields(
                            sourceTableLastStageSchema.getFields(), normalizeString(tokens2[0]));
                    for (SchemaFieldName field : matchingFields) {
                        String alias = sourceTableLastStage.toLowerCase() + "_" + field.getName();
                        if (!aliasIsInFeatureDagEntryMap(alias)) {
                            continue;
                        }
                        if (index > 0) {
                            selectedFields.append(",");
                        }
                        boolean result = schemaSet.add(normalizeString(alias));
                        if (!result) {
                            continue;
                        }
                        selectedFields.append(sourceTableLastStage + "." + normalizeString(field.getName()) + " as "
                                + normalizeString(alias));
                        
                        currentSchemaFields = new SchemaField();
                        currentSchemaFields.setName(normalizeString(alias));
                        currentSchemaFields.setType(sourceDataTypeMap.get(normalizeString(field.getName())));
                        currentSchemaFieldList.add(currentSchemaFields);
                        added = true;
                        index++;
                    }
                }
            }
            if (!added) {
                boolean result = schemaSet.add(normalizeString(tokens2[1]));
                if (!result) {
                    continue;
                }
                currentSchemaFields = new SchemaField();
                currentSchemaFields.setName(normalizeString(tokens2[1]));
                currentSchemaFields.setType(sourceDataTypeMap.get(normalizeString(tokens2[0])));
                currentSchemaFieldList.add(currentSchemaFields);
                if (index > 0) {
                    selectedFields.append(",");
                }
                selectedFields.append(sourceTableLastStage + "." + normalizeString(tokens2[0]) + " as "
                        + normalizeString(tokens2[1]));
                index++;
            }
        }
        currentSchema.setFields(currentSchemaFieldList.toArray(new SchemaFieldName[0]));
        pluginProperties.put("joinKeys",
                sourceTableLastStage + "." + sourceJoinKey + " = " + destTableLastStage + "." + destJoinKey);
        pluginProperties.put("requiredInputs", sourceTableLastStage + ", " + destTableLastStage);
        pluginProperties.put("selectedFields", selectedFields.toString());
        if (enablePartitions) {
            pluginProperties.put("numPartitions", NUM_PARTITIONS);
        }
        List<InOutSchema> inputSchemaList = generateInputSchema(sourceTableLastStage, sourceTableLastStageSchema,
                pipelineNode);
        inputSchemaList.addAll(generateInputSchema(destTableLastStage, destTableLastStageSchema, pipelineNode));
        pipelineNode.setInputSchema(inputSchemaList);
        generateOutputSchema("etlSchemaBody", currentSchema, pipelineNode);
        schemaMap.put(stageName, currentSchema);
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
        } else {
            stageName = lastStageMapForTable.get(sourceTables) + "_";
        }
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
        Schema currentSchema = new Schema();
        Schema lastSchema = schemaMap.get(lastStageMapForTable.get(sourceTables));
        List<SchemaFieldName> currentSchemaFieldList = new LinkedList<>();
        SchemaField currentSchemaField = new SchemaField();
        if (pluginSummary.getName().equals("RowTransform")) {
            currentSchema.setName(lastSchema.getName());
            currentSchema.setType(lastSchema.getType());
            Map<String, String> currentSchemaMap = new HashMap<String, String>();
            for (SchemaFieldName field : lastSchema.getFields()) {
                currentSchemaMap.put(field.getName(), getSchemaType(field));
                currentSchemaFieldList.add(field);
            }
            
            StringBuilder primitives = new StringBuilder();
            int index = 0;
            for (String columnOperation : columnOperations) {
                String token[] = columnOperation.split(",");
                token[0] = token[0].toLowerCase();
                
                boolean added = false;
                for (Map.Entry<String, Set<String>> entry : categoricalColumnsToBeChecked.entrySet()) {
                    String dictColumn = entry.getKey();
                    if (token[0].contains(dictColumn)) {
                        List<SchemaFieldName> matchingFields = getAllMatchingSchemaFields(lastSchema.getFields(),
                                token[0]);
                        for (SchemaFieldName field : matchingFields) {
                            String alias = token[2].toLowerCase() + "_" + field.getName() + "_";
                            if (!aliasIsInFeatureDagEntryMap(alias)) {
                                continue;
                            }
                            if (index > 0) {
                                primitives.append(",");
                            }
                            String outputType = getOutputType(currentSchemaMap.get(field.getName()), token[2],
                                    pluginSummary.getName(), functionDataTypeInfoMapTransform);
                            primitives.append(alias + ":" + token[2].toUpperCase() + "(" + field.getName() + ")");
                            currentSchemaField = new SchemaField();
                            currentSchemaField.setName(alias);
                            currentSchemaField.setType(outputType);
                            currentSchemaFieldList.add(currentSchemaField);
                            index++;
                            added = true;
                        }
                    }
                }
                if (!added) {
                    if (index > 0) {
                        primitives.append(",");
                    }
                    String outputType = getOutputType(currentSchemaMap.get(token[0]), token[2], pluginSummary.getName(),
                            functionDataTypeInfoMapTransform);
                    primitives.append(token[1] + ":" + token[2].toUpperCase() + "(" + token[0] + ")");
                    if (!currentSchemaMap.containsKey(token[0])) {
                        throw new IllegalStateException("Input Column is not present in last stage schema");
                    }
                    currentSchemaField = new SchemaField();
                    currentSchemaField.setName(token[1]);
                    currentSchemaField.setType(outputType);
                    currentSchemaFieldList.add(currentSchemaField);
                }
                index++;
            }
            pluginProperties.put("primitives", primitives.toString());
            if (enablePartitions) {
                pluginProperties.put("numPartitions", NUM_PARTITIONS);
            }
        }
        currentSchema.setFields(currentSchemaFieldList.toArray(new SchemaFieldName[0]));
        setInputOutputSchema(lastSchema, currentSchema, lastStageMapForTable.get(sourceTables), pipelineNode);
        putInConnection(lastStageMapForTable.get(sourceTables), stageName);
        isUsedStage.add(lastStageMapForTable.get(sourceTables));
        isUsedStage.add(stageName);
        schemaMap.put(stageName, currentSchema);
        stageMap.put(stageName, pipelineNode);
        lastStageMapForTable.put(sourceTables, stageName);
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
        pipelineNode.setOutputSchema(GSON_OBJ.toJson(outputSchema));
    }
    
    private List<InOutSchema> generateInputSchema(String inputName, Schema inputSchema, PipelineNode pipelineNode) {
        List<InOutSchema> inputSchemaList = new LinkedList<InOutSchema>();
        InOutSchema inSchema = new InOutSchema();
        inSchema.setName(inputName);
        inSchema.setSchema(GSON_OBJ.toJson(inputSchema));
        inputSchemaList.add(inSchema);
        return inputSchemaList;
    }
    
    private String getOutputType(String inputDataType, String function, String pluginName,
            Map<String, Map<String, Map<String, String>>> functionDataTypeInfoMap) {
        Map<String, String> inputOutputDataType = functionDataTypeInfoMap.get(pluginName).get(function);
        String outputType = inputOutputDataType.get("*");
        if (outputType != null) {
            if (outputType.equals("same")) {
                return inputDataType;
            } else {
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
        tmp.put("schema", GSON_OBJ.toJson(schema));
        tmp.put("schema.row.field", "ts");
        
        // System.out.println(GSON_OBJ.toJson(tmp));
        
        List<Connection> connections = new LinkedList<Connection>();
        Connection con1 = new Connection();
        con1.setFrom("twitterSource");
        con1.setTo("dropProjector");
        connections.add(con1);
        
        Connection con2 = new Connection();
        con2.setFrom("dropProjector");
        con2.setTo("tableSink");
        connections.add(con2);
        // System.out.println(GSON_OBJ.toJson(connections));
        
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
        System.out.println("StageNode = " + GSON_OBJ.toJson(node));
        System.out.println("PipelineNode = " + GSON_OBJ.toJson(node2));
    }
    
}
