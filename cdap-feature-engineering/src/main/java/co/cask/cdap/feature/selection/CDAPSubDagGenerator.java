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
package co.cask.cdap.feature.selection;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.featureengineer.AutoFeatureGenerator;
import co.cask.cdap.featureengineer.pipeline.CDAPPipelineGenerator;
import co.cask.cdap.featureengineer.pipeline.pojo.CDAPPipelineInfo;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchema;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchemaField;
import co.cask.cdap.featureengineer.proto.FeatureGenerationRequest;
import co.cask.cdap.featureengineer.request.pojo.MultiFieldAggregationInput;
import co.cask.cdap.featureengineer.request.pojo.MultiSchemaColumn;
import co.cask.cdap.featureengineer.request.pojo.SchemaColumn;
import co.cask.cdap.proto.artifact.PluginSummary;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author bhupesh.goel
 *
 */
public class CDAPSubDagGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(CDAPSubDagGenerator.class);

    static final Gson GSON_OBJ = new GsonBuilder().setPrettyPrinting().create();
    private Map<String, Set<String>> columnDictionarySet;
    private Map<String, Set<String>> multiColumnDictionarySet;
    private Map<String, Set<String>> featureDagEntryDictionaryMap;
    private Map<String, NullableSchema> dataSchema;
    private Map<String, String> multiInputColumnTableMap;
    private Set<String> multiInputColumnSet;
    private List<String> trainingWindows;
    private AutoFeatureGenerator autoFeatureGenerator;
    private ClientConfig clientConfig;
    private FeatureGenerationRequest featureGenerationRequest;
    private String featureDag;
    private Map<String, CDAPPipelineInfo> wranglerPluginConfigMap;

    private CDAPSubDagGeneratorOutput emitOutputAndInitState(String featureDag, String trainingWindow) {
        List<String> categoricalColumnDictionary = createCategoricalDictionary(columnDictionarySet);
        CDAPSubDagGeneratorOutput output = new CDAPSubDagGeneratorOutput(categoricalColumnDictionary,
                multiColumnDictionarySet, multiInputColumnTableMap, multiInputColumnSet, featureDagEntryDictionaryMap);
        output.setFeatureSubDag(featureDag);
        output.setTrainingWindow(trainingWindow);
        this.columnDictionarySet = new HashMap<>();
        this.featureDagEntryDictionaryMap = new HashMap<>();
        this.multiColumnDictionarySet = new HashMap<>();
        this.multiInputColumnSet = new HashSet<>();
        return output;
    }

    /**
     * 
     * @author bhupesh.goel
     *
     */
    public static class CDAPSubDagGeneratorOutput {
        private List<String> categoricalColumnDictionary;
        private Map<String, Set<String>> multiColumnDictionarySet;
        private Map<String, Set<String>> featureDagEntryDictionaryMap;
        private Map<String, String> multiInputColumnTableMap;
        private Set<String> multiInputColumnSet;
        private String trainingWindow;
        private String featureSubDag;

        public CDAPSubDagGeneratorOutput(List<String> categoricalColumnDictionary,
                Map<String, Set<String>> multiColumnDictionarySet, Map<String, String> multiInputColumnTableMap,
                Set<String> multiInputColumnSet, Map<String, Set<String>> featureDagEntryDictionaryMap) {
            this.categoricalColumnDictionary = categoricalColumnDictionary;
            this.multiColumnDictionarySet = multiColumnDictionarySet;
            this.multiInputColumnTableMap = multiInputColumnTableMap;
            this.multiInputColumnSet = multiInputColumnSet;
            this.featureDagEntryDictionaryMap = featureDagEntryDictionaryMap;
        }

        /**
         * @return the featureDagEntryDictionaryMap
         */
        public Map<String, Set<String>> getFeatureDagEntryDictionaryMap() {
            return featureDagEntryDictionaryMap;
        }

        /**
         * @param featureDagEntryDictionaryMap
         *            the featureDagEntryDictionaryMap to set
         */
        public void setFeatureDagEntryDictionaryMap(Map<String, Set<String>> featureDagEntryDictionaryMap) {
            this.featureDagEntryDictionaryMap = featureDagEntryDictionaryMap;
        }

        /**
         * @return the columnDictionarySet
         */
        public List<String> getCategoricalColumnDictionary() {
            return categoricalColumnDictionary;
        }

        /**
         * @param categoricalColumnDictionary
         *            the columnDictionarySet to set
         */
        public void setCategoricalColumnDictionary(List<String> categoricalColumnDictionary) {
            this.categoricalColumnDictionary = categoricalColumnDictionary;
        }

        /**
         * @return the multiColumnDictionarySet
         */
        public Map<String, Set<String>> getMultiColumnDictionarySet() {
            return multiColumnDictionarySet;
        }

        /**
         * @param multiColumnDictionarySet
         *            the multiColumnDictionarySet to set
         */
        public void setMultiColumnDictionarySet(Map<String, Set<String>> multiColumnDictionarySet) {
            this.multiColumnDictionarySet = multiColumnDictionarySet;
        }

        /**
         * @return the multiInputColumnTableMap
         */
        public Map<String, String> getMultiInputColumnTableMap() {
            return multiInputColumnTableMap;
        }

        /**
         * @param multiInputColumnTableMap
         *            the multiInputColumnTableMap to set
         */
        public void setMultiInputColumnTableMap(Map<String, String> multiInputColumnTableMap) {
            this.multiInputColumnTableMap = multiInputColumnTableMap;
        }

        /**
         * @return the multiInputColumnSet
         */
        public Set<String> getMultiInputColumnSet() {
            return multiInputColumnSet;
        }

        /**
         * @param multiInputColumnSet
         *            the multiInputColumnSet to set
         */
        public void setMultiInputColumnSet(Set<String> multiInputColumnSet) {
            this.multiInputColumnSet = multiInputColumnSet;
        }

        /**
         * @return the trainingWindow
         */
        public String getTrainingWindow() {
            return trainingWindow;
        }

        /**
         * @param trainingWindow
         *            the trainingWindow to set
         */
        public void setTrainingWindow(String trainingWindow) {
            this.trainingWindow = trainingWindow;
        }

        /**
         * @return the featureSubDag
         */
        public String getFeatureSubDag() {
            return featureSubDag;
        }

        /**
         * @param featureSubDag
         *            the featureSubDag to set
         */
        public void setFeatureSubDag(String featureSubDag) {
            this.featureSubDag = featureSubDag;
        }

    }

    public CDAPSubDagGenerator(final String featureDag, Map<String, NullableSchema> dataSchemaMap,
            Map<String, CDAPPipelineInfo> wranglerPluginConfigMap, FeatureGenerationRequest featureGenerationRequest,
            String[] hostAndPort) {
        this.columnDictionarySet = new HashMap<>();
        this.featureDagEntryDictionaryMap = new HashMap<>();
        this.multiColumnDictionarySet = new HashMap<>();
        this.multiInputColumnSet = new HashSet<>();
        this.clientConfig = ClientConfig.builder()
                .setConnectionConfig(new ConnectionConfig(hostAndPort[0], Integer.parseInt(hostAndPort[1]), false))
                .build();
        this.multiInputColumnTableMap = populateMultiInputColumnMap(
                featureGenerationRequest.getMultiFieldTransformationFunctionInputs(),
                featureGenerationRequest.getMultiFieldAggregationFunctionInputs());
        this.dataSchema = dataSchemaMap;
        this.trainingWindows = new LinkedList<String>();
        for (Integer window : featureGenerationRequest.getTrainingWindows()) {
            this.trainingWindows.add(String.valueOf(window));
        }
        this.featureGenerationRequest = featureGenerationRequest;
        this.featureDag = featureDag;
        this.wranglerPluginConfigMap = wranglerPluginConfigMap;
        this.autoFeatureGenerator = new AutoFeatureGenerator(featureGenerationRequest, dataSchemaMap,
                wranglerPluginConfigMap);
    }

    /**
     * @param features
     * @param featureSelectionPipeline
     * @param featureGenerationRequest
     * @param wranglerPluginConfigMap
     * @param hostAndPort
     * @param args
     * @throws IOException
     * @throws UnauthorizedException
     * @throws ArtifactNotFoundException
     * @throws UnauthenticatedException
     * @throws InterruptedException
     */
    public void triggerCDAPPipelineGeneration(List<String> features, String featureSelectionPipeline)
            throws Exception {
        Set<String> manualMultiInputFeatures = getManualMultiInputFeatures();
        Map<String, CDAPSubDagGeneratorOutput> dagGeneratorOutputMap = generateFilteredFeatureDag(featureDag, features,
                manualMultiInputFeatures);
        LOG.debug("Feature DAG Map = " + dagGeneratorOutputMap);
        for (Map.Entry<String, CDAPSubDagGeneratorOutput> entry : dagGeneratorOutputMap.entrySet()) {
            LOG.debug("Original Feature Dag length=" + featureDag.length() + "\nNew feature dag length="
                    + entry.getValue().featureSubDag.length() + " for timewindow=" + entry.getKey());
        }

        LOG.debug("multiInputColumnSet=" + this.multiInputColumnSet);
        generateCDAPPipelineForDAG(dagGeneratorOutputMap, featureSelectionPipeline);
    }

    private Set<String> getManualMultiInputFeatures()
            throws UnauthenticatedException, ArtifactNotFoundException, IOException, UnauthorizedException {
        Map<String, PluginSummary> multiInputAggregatePluginFunctionMap = new HashMap<String, PluginSummary>();
        Map<String, PluginSummary> aggregatePluginFunctionMap = new HashMap<String, PluginSummary>();

        Set<String> typeSet = new HashSet<String>();
        List<String> entityNames = new LinkedList<String>();
        List<NullableSchema> dataSchemaList = new LinkedList<NullableSchema>();
        autoFeatureGenerator.parseDataSchemaInJson(typeSet, entityNames, dataSchemaList);

        List<PluginSummary> pluginSummariesBatchAggregator = new LinkedList<PluginSummary>();
        List<PluginSummary> pluginSummariesTransform = new LinkedList<PluginSummary>();
        autoFeatureGenerator.getCDAPPluginSummary(pluginSummariesBatchAggregator, pluginSummariesTransform,
                clientConfig);

        autoFeatureGenerator.getPrimitives(pluginSummariesBatchAggregator, typeSet, aggregatePluginFunctionMap,
                this.featureGenerationRequest.getCategoricalColumns(), multiInputAggregatePluginFunctionMap, false);

        Map<String, Map<String, List<String>>> appliedAggFunctionsWithArguments = autoFeatureGenerator
                .findMatchingMultiInputPrimitives(
                        this.featureGenerationRequest.getMultiFieldAggregationFunctionInputs(),
                        multiInputAggregatePluginFunctionMap);
        Set<String> set = new HashSet<String>();
        for (Map.Entry<String, Map<String, List<String>>> entry : appliedAggFunctionsWithArguments.entrySet()) {
            set.addAll(entry.getValue().keySet());
        }
        return set;
    }

    private static List<String> createCategoricalDictionary(Map<String, Set<String>> columnDictionarySet) {
        List<String> categoricalColumnDictionary = new LinkedList<>();

        for (Map.Entry<String, Set<String>> entry : columnDictionarySet.entrySet()) {

            Set<String> dictionarySet = entry.getValue();
            if (dictionarySet != null && !dictionarySet.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                sb.append(entry.getKey());
                sb.append("*");
                int index = 0;
                for (String dictionary : dictionarySet) {
                    if (index > 0) {
                        sb.append(";");
                    }
                    dictionary = dictionary.replaceAll(":", "^");
                    dictionary = dictionary.replaceAll("\\s+", "^^");
                    sb.append(dictionary);
                    index++;
                }
                if (sb.length() > 0) {
                    categoricalColumnDictionary.add(sb.toString());
                }
            }
        }
        return categoricalColumnDictionary;
    }

    private void generateCDAPPipelineForDAG(Map<String, CDAPSubDagGeneratorOutput> dagGeneratorOutputMap,
            String featureSelectionPipeline) throws Exception {
        Map<String, PluginSummary> aggregatePluginFunctionMap = new HashMap<String, PluginSummary>();
        Map<String, PluginSummary> transformPluginFunctionMap = new HashMap<String, PluginSummary>();

        Map<String, PluginSummary> multiInputAggregatePluginFunctionMap = new HashMap<String, PluginSummary>();
        Map<String, PluginSummary> multiInputTransformPluginFunctionMap = new HashMap<String, PluginSummary>();

        Set<String> typeSet = new HashSet<String>();
        List<String> entityNames = new LinkedList<String>();
        List<NullableSchema> dataSchemaList = new LinkedList<NullableSchema>();
        autoFeatureGenerator.parseDataSchemaInJson(typeSet, entityNames, dataSchemaList);

        List<PluginSummary> pluginSummariesBatchAggregator = new LinkedList<PluginSummary>();
        List<PluginSummary> pluginSummariesTransform = new LinkedList<PluginSummary>();
        autoFeatureGenerator.getCDAPPluginSummary(pluginSummariesBatchAggregator, pluginSummariesTransform,
                clientConfig);

        autoFeatureGenerator.getPrimitives(pluginSummariesBatchAggregator, typeSet, aggregatePluginFunctionMap,
                this.featureGenerationRequest.getCategoricalColumns(), multiInputAggregatePluginFunctionMap, false);
        autoFeatureGenerator.getPrimitives(pluginSummariesTransform, typeSet, transformPluginFunctionMap, null,
                multiInputTransformPluginFunctionMap, false);

        Map<String, Map<String, List<String>>> appliedTransFunctionsWithArguments = autoFeatureGenerator
                .findMatchingMultiInputPrimitives(
                        this.featureGenerationRequest.getMultiFieldTransformationFunctionInputs(),
                        multiInputTransformPluginFunctionMap);
        Map<String, Map<String, List<String>>> appliedAggFunctionsWithArguments = autoFeatureGenerator
                .findMatchingMultiInputPrimitives(
                        this.featureGenerationRequest.getMultiFieldAggregationFunctionInputs(),
                        multiInputAggregatePluginFunctionMap);

        LOG.debug("Merged Feature Dag = " + mergeFeatureDag(dagGeneratorOutputMap));
        CDAPPipelineGenerator pipelineGenerator = new CDAPPipelineGenerator(aggregatePluginFunctionMap,
                transformPluginFunctionMap, entityNames, featureGenerationRequest.getTargetEntity(),
                featureGenerationRequest.getTargetEntityFieldId(), featureGenerationRequest.getWindowEndTime(),
                featureGenerationRequest.getTrainingWindows(), featureGenerationRequest.getTimeIndexColumns(),
                multiInputAggregatePluginFunctionMap, multiInputTransformPluginFunctionMap,
                appliedAggFunctionsWithArguments, appliedTransFunctionsWithArguments,
                featureGenerationRequest.getIndexes(), dagGeneratorOutputMap, featureSelectionPipeline,
                featureGenerationRequest.getRelationShips(), featureGenerationRequest.getCreateEntities());

        CDAPPipelineInfo pipelineInfo = pipelineGenerator.generateCDAPPipeline(this.dataSchema,
                wranglerPluginConfigMap);

        File cdapPipelineFile = writeDataToTempFile(GSON_OBJ.toJson(pipelineInfo), "cdap-pipeline");
        String cdapPipelineFileName = cdapPipelineFile.getAbsolutePath();

        try {
            autoFeatureGenerator.uploadPipelineFileToCDAP(cdapPipelineFileName, clientConfig, pipelineInfo.getName());
            LOG.info("successfully uploaded cdap pipeline " + pipelineInfo.getName() + " to CDAP server. ");
        } catch (Throwable ex) {
            if (ex.getMessage().contains("duplicate key")) {
                LOG.info("successfully uploaded cdap pipeline " + pipelineInfo.getName()
                        + " to CDAP server with exeption message = " + ex.getMessage());
            } else {
                LOG.error("Got error while generating subdag for selected features", ex);
                throw ex;
            }

        }

    }

    File writeDataToTempFile(String data, String key) throws IOException {
        File tmpFile = File.createTempFile("temp-feature-selection-key", ".tmp");
        FileWriter writer = new FileWriter(tmpFile);
        writer.write(data);
        writer.close();
        return tmpFile;
    }

    private static String mergeFeatureDag(Map<String, CDAPSubDagGeneratorOutput> dagGeneratorOutputMap) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, CDAPSubDagGeneratorOutput> entry : dagGeneratorOutputMap.entrySet()) {
            sb.append(entry.getKey() + "\n");
            sb.append(entry.getValue().featureSubDag + "\n");
        }
        return sb.toString();
    }

    private Map<String, String> populateMultiInputColumnMap(
            List<MultiSchemaColumn> multiFieldTransformationFunctionInputs,
            List<MultiFieldAggregationInput> multiFieldAggregationFunctionInputs) {
        Map<String, String> multiInputMap = new HashMap<>();
        if ((multiFieldAggregationFunctionInputs == null || multiFieldAggregationFunctionInputs.isEmpty())
                && (multiFieldTransformationFunctionInputs == null
                        || multiFieldTransformationFunctionInputs.isEmpty())) {
            return multiInputMap;
        }
        for (MultiSchemaColumn multiSchema : multiFieldTransformationFunctionInputs) {
            StringBuilder sb = new StringBuilder();
            String table = "";
            for (SchemaColumn column : multiSchema.getColumns()) {
                table = column.getTable();
                if (sb.length() > 0) {
                    sb.append("_");
                }
                sb.append(column.getColumn());
            }
            multiInputMap.put(sb.toString(), table);
        }
        for (MultiFieldAggregationInput multiFieldInput : multiFieldAggregationFunctionInputs) {
            StringBuilder sb = new StringBuilder();
            String table = "";
            for (SchemaColumn column : multiFieldInput.getSourceColumns()) {
                table = column.getTable();
                if (sb.length() > 0) {
                    sb.append("_");
                }
                sb.append(column.getColumn());
            }
            multiInputMap.put(sb.toString(), table);
        }
        return multiInputMap;
    }

    public Map<String, CDAPSubDagGeneratorOutput> generateFilteredFeatureDag(final String featureDag,
            final List<String> originalFeatureSet, Set<String> manualMultiInputFeatures) throws IOException {
        if (featureDag == null || featureDag.isEmpty()) {
            return new HashMap<>();
        }
        System.out.println("Original Feature size = " + originalFeatureSet.size());
        Map<String, Set<String>> trainingWiseFeatureSet = breakRandomFeatureSetTrainingWindowWise(originalFeatureSet);
        Map<String, CDAPSubDagGeneratorOutput> dagGeneratorOutputMap = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : trainingWiseFeatureSet.entrySet()) {
            String filteredFeatureDag = getFilteredFeatureDag(featureDag, entry.getValue(), manualMultiInputFeatures);
            CDAPSubDagGeneratorOutput output = emitOutputAndInitState(filteredFeatureDag, entry.getKey());
            dagGeneratorOutputMap.put(entry.getKey(), output);
        }

        LOG.debug("Single column dictionary = " + columnDictionarySet);
        LOG.debug("Multi Input Column Dictionary = " + multiColumnDictionarySet + "\nfeatureDagEntryDictionaryMap="
                + featureDagEntryDictionaryMap);
        return dagGeneratorOutputMap;
    }

    private Map<String, Set<String>> breakRandomFeatureSetTrainingWindowWise(List<String> randomFeatureSet) {
        Map<String, Set<String>> trainingWiseFeatureSet = new HashMap<>();
        String singleKey = "0";
        if (this.trainingWindows == null || this.trainingWindows.size() <= 1) {
            trainingWiseFeatureSet.put(singleKey, new HashSet<String>(randomFeatureSet));
            return trainingWiseFeatureSet;
        }
        if (this.trainingWindows.size() == 1) {
            singleKey = trainingWindows.get(0);
        }
        Set<String> trainingWindowSet = new HashSet<String>(this.trainingWindows);
        for (String feature : randomFeatureSet) {
            Integer suffix = getLastSuffixedNumber(feature);
            if (suffix == null) {
                System.out.println("empty feature with no suffix=" + feature);
                continue;
            }
            if (trainingWindowSet.contains(suffix + "")) {
                addEntryInMapSet(trainingWiseFeatureSet, suffix + "", feature);
            }
        }
        return trainingWiseFeatureSet;
    }

    private Integer getLastSuffixedNumber(String feature) {
        int index = feature.length() - 1;
        while (index > -1 && Character.isDigit(feature.charAt(index))) {
            index--;
        }
        if (feature.charAt(index) != '_') {
            return null;
        }
        return Integer.parseInt(feature.substring(index + 1));
    }

    private String getFilteredFeatureDag(String featureDag, Set<String> randomFeatureList,
            Set<String> manualMultiInputFeatures) {
        StringBuilder sb = new StringBuilder();
        String featureList[] = featureDag.split("\n");
        for (int i = 0; i < featureList.length; i++) {
            String feature = featureList[i].trim();
            try {
                Integer.parseInt(feature);
                sb.append(feature);
                sb.append("\n");
                continue;
            } catch (Exception e) {
                String sourceTables = feature.trim();
                sb.append(feature.trim());
                sb.append("\n");
                String operations = featureList[i + 1].trim();
                operations = operations.substring(1, operations.length() - 1).trim();
                String[] tokens = operations.split(", ");
                int startIndex = 1;
                String sourceTable = "";
                if (sourceTables.contains("->")) {
                    startIndex = 2;
                    sourceTable = sourceTables.split("->")[0];
                } else {
                    sourceTable = sourceTables;
                }
                int sbIndex = 0;
                sb.append("[");
                for (int j = 0; j < startIndex; j++) {
                    if (sbIndex > 0) {
                        sb.append(", ");
                    }
                    sb.append(tokens[j]);
                    sbIndex++;
                }
                for (int j = startIndex; j < tokens.length; j++) {
                    String tokens2[] = tokens[j].substring(1, tokens[j].length() - 1).trim().split(",");
                    String sourceFeature = normalizeString(tokens2[0]);
                    String destinationFeature = normalizeString(tokens2[1]);
                    if (presentInSelectedFeatures(destinationFeature, randomFeatureList, sourceTable, sourceFeature,
                            manualMultiInputFeatures)) {
                        if (sbIndex > 0) {
                            sb.append(", ");
                        }
                        sb.append(tokens[j]);
                    }
                }
                sb.append("]\n");

                i++;
            }
        }
        return sb.toString();
    }

    private boolean presentInSelectedFeatures(String featureDagEntry, Set<String> randomFeatureList, String sourceTable,
            String sourceFeature, Set<String> manualMultiInputFeatures) {
        int matchingCount = 0;
        for (String feature : randomFeatureList) {
            if (feature.startsWith(sourceFeature) && manualMultiInputFeatures.contains(sourceFeature)) {
                int tIndex = feature.indexOf(sourceFeature);
                processFeatureDagEntry(feature, tIndex, sourceFeature, sourceTable, sourceFeature);
            }
            int index = feature.indexOf(featureDagEntry);
            if (index < 0 || (index > 0 && feature.charAt(index - 1) != '_')) {
                continue;
            }
            processFeatureDagEntry(feature, index, featureDagEntry, sourceTable, sourceFeature);
            matchingCount++;
        }
        return matchingCount > 0 ? true : false;
    }

    private void processFeatureDagEntry(String feature, int index, String featureDagEntry, String sourceTable,
            String sourceFeature) {
        String dictionaryPartOfFeature = feature.substring(index + featureDagEntry.length());
        String dictionaryPartOnly = trimByChar(dictionaryPartOfFeature, '_');
        dictionaryPartOnly = dictionaryPartOnly.replaceAll(":", "^");
        dictionaryPartOnly = dictionaryPartOnly.replaceAll("\\s+", "^^");
        String[] dictionary = dictionaryPartOnly.trim().split("__");
        if (dictionaryPartOnly.isEmpty()) {
            dictionary = new String[0];
        }
        List<String> matchingTableColumnsInFeature = new ArrayList<String>();
        List<Integer> matchingTableColumnPositionsInFeature = new ArrayList<Integer>();

        List<String> columns = getColumnNames(dataSchema.get(sourceTable).getFields());
        String currentSourceTable = getLastIndexOfTableName(feature.substring(0, index + featureDagEntry.length()),
                dataSchema.keySet());
        if (currentSourceTable != null && !currentSourceTable.isEmpty()) {
            columns = getColumnNames(dataSchema.get(currentSourceTable).getFields());
        } else {
            currentSourceTable = sourceTable;
        }
        for (String column : columns) {
            index = feature.indexOf("_" + column + "_");
            if (index < 0) {
                continue;
            }
            matchingTableColumnsInFeature.add(column);
            matchingTableColumnPositionsInFeature.add(index + 1);
        }
        if (matchingTableColumnPositionsInFeature.isEmpty() && dictionary.length > 0) {
            for (String manualFeature : multiInputColumnTableMap.keySet()) {
                if (featureDagEntry.contains(manualFeature)) {
                    currentSourceTable = multiInputColumnTableMap.get(manualFeature);
                    columns = getColumnNames(dataSchema.get(currentSourceTable).getFields());
                    for (String column : columns) {
                        index = feature.indexOf("_" + column + "_");
                        if (index < 0) {
                            continue;
                        }
                        matchingTableColumnsInFeature.add(column);
                        matchingTableColumnPositionsInFeature.add(index + 1);
                    }
                }
            }
        }
        if (matchingTableColumnPositionsInFeature.size() != dictionary.length && dictionary.length > 0) {

            throw new IllegalStateException(
                    "Identified columns doesn't match with identified dictionary elements. featuredagenty="
                            + featureDagEntry + " feature = " + feature + " dictionaryPartOnly=" + dictionaryPartOnly
                            + " matchingTableColumnsInFeature=" + matchingTableColumnsInFeature);
        } else if (matchingTableColumnPositionsInFeature.size() > 1 && dictionary.length == 0) {
            this.multiInputColumnSet.add(sourceFeature);
        }
        if (matchingTableColumnPositionsInFeature.size() == dictionary.length && dictionary.length > 0) {
            sortColumnsPositionsByIndex(matchingTableColumnsInFeature, matchingTableColumnPositionsInFeature);
            if (matchingTableColumnsInFeature.size() == 1) {
                addEntryInMapSet(columnDictionarySet, currentSourceTable + "." + matchingTableColumnsInFeature.get(0),
                        dictionary[0]);
                addEntryInMapSet(featureDagEntryDictionaryMap, trimByChar(featureDagEntry, '_'), dictionary[0]);
            } else {
                StringBuilder columnSb = new StringBuilder();
                StringBuilder dictSb = new StringBuilder();
                for (int i = 0; i < matchingTableColumnsInFeature.size(); i++) {
                    String column = matchingTableColumnsInFeature.get(i);
                    if (i > 0) {
                        columnSb.append(" ");
                        dictSb.append("__");

                    }
                    columnSb.append(column);
                    dictSb.append(dictionary[i]);
                    addEntryInMapSet(columnDictionarySet,
                            currentSourceTable + "." + matchingTableColumnsInFeature.get(i), dictionary[i]);
                }
                Set<String> dictionarySet = multiColumnDictionarySet.get(columnSb.toString());
                if (dictionarySet == null) {
                    dictionarySet = new HashSet<>();
                    multiColumnDictionarySet.put(columnSb.toString(), dictionarySet);
                }
                dictionarySet.add(dictSb.toString());
                addEntryInMapSet(featureDagEntryDictionaryMap, trimByChar(sourceFeature, '_'), dictSb.toString());
                addEntryInMapSet(featureDagEntryDictionaryMap, trimByChar(featureDagEntry, '_'), dictSb.toString());
                this.multiInputColumnSet.add(sourceFeature);
            }
        }
    }

    private List<String> getColumnNames(NullableSchemaField[] fields) {
        List<String> columnList = new LinkedList<String>();
        for (NullableSchemaField field : fields) {
            columnList.add(field.getName());
        }
        return columnList;
    }

    private void addEntryInMapSet(Map<String, Set<String>> map, String mapKey, String value) {
        Set<String> dictionarySet = map.get(mapKey);
        if (dictionarySet == null) {
            dictionarySet = new HashSet<>();
            map.put(mapKey, dictionarySet);
        }
        dictionarySet.add(value);
    }

    private String getLastIndexOfTableName(String featureSubstring, Set<String> keySet) {
        int maxIndex = -1;
        String toReturnTable = "";
        for (String table : keySet) {
            int index = featureSubstring.indexOf(table);
            if (index > maxIndex) {
                maxIndex = index;
                toReturnTable = table;
            }
        }
        return toReturnTable;
    }

    private void sortColumnsPositionsByIndex(List<String> matchingTableColumnsInFeature,
            List<Integer> matchingTableColumnPositionsInFeature) {
        for (int i = 0; i < matchingTableColumnPositionsInFeature.size(); i++) {
            for (int j = i + 1; j < matchingTableColumnPositionsInFeature.size(); j++) {
                int iPos = matchingTableColumnPositionsInFeature.get(i);
                int jPos = matchingTableColumnPositionsInFeature.get(j);
                if (jPos < iPos) {
                    matchingTableColumnPositionsInFeature.set(i, jPos);
                    matchingTableColumnPositionsInFeature.set(j, iPos);
                    String tmp = matchingTableColumnsInFeature.get(i);
                    matchingTableColumnsInFeature.set(i, matchingTableColumnsInFeature.get(j));
                    matchingTableColumnsInFeature.set(j, tmp);
                }
            }
        }
    }

    private String trimByChar(String dictionaryPartOfFeature, char trimChar) {
        int st = 0, end = dictionaryPartOfFeature.length() - 1;
        while (st < dictionaryPartOfFeature.length() && dictionaryPartOfFeature.charAt(st) == trimChar) {
            st++;
        }
        if (this.trainingWindows != null && this.trainingWindows.size() > 1) {
            while (end >= 0 && Character.isDigit(dictionaryPartOfFeature.charAt(end))) {
                end--;
            }
        }
        while (end >= 0 && dictionaryPartOfFeature.charAt(end) == trimChar) {
            end--;
        }
        if (st > end) {
            return "";
        }
        return dictionaryPartOfFeature.substring(st, end + 1);
    }

    private String normalizeString(final String input) {
        return input.replaceAll("[\\.()]", "_").toLowerCase();
    }

    public Map<String, Set<String>> getColumnDictionarySet() {
        return columnDictionarySet;
    }

    public void setColumnDictionarySet(Map<String, Set<String>> columnDictionarySet) {
        this.columnDictionarySet = columnDictionarySet;
    }

    public Map<String, Set<String>> getMultiColumnDictionarySet() {
        return multiColumnDictionarySet;
    }

    public void setMultiColumnDictionarySet(Map<String, Set<String>> multiColumnDictionarySet) {
        this.multiColumnDictionarySet = multiColumnDictionarySet;
    }
}
