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
import co.cask.cdap.featureengineer.enums.PipelineType;
import co.cask.cdap.featureengineer.pipeline.pojo.CDAPPipelineInfo;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchema;
import co.cask.cdap.featureengineer.proto.FeatureGenerationRequest;
import co.cask.cdap.featureengineer.proto.FeatureSelectionRequest;
import co.cask.cdap.featureengineer.proto.FeatureStatsPriorityNode;
import co.cask.cdap.featureengineer.proto.FilterFeaturesByStatsRequest;
import co.cask.cdap.featureengineer.request.pojo.CompositeType;
import co.cask.cdap.featureengineer.request.pojo.DataSchemaNameList;
import co.cask.cdap.featureengineer.request.pojo.StatsFilter;
import co.cask.cdap.featureengineer.request.pojo.StatsFilterType;
import co.cask.cdap.featureengineer.response.pojo.FeatureStats;
import co.cask.cdap.featureengineer.response.pojo.SelectedFeatureStats;
import co.cask.cdap.featureengineer.utils.JSONInputParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * @author bhupesh.goel
 *
 */
public class ManualFeatureSelectionServiceHandler extends BaseServiceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ManualFeatureSelectionServiceHandler.class);

    private StatsFilter defaultFilter;
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
    @Property
    private final String pipelineNameTableName;

    private KeyValueTable dataSchemaTable;
    private KeyValueTable pluginConfigTable;
    private KeyValueTable featureDAGTable;
    private KeyValueTable featureEngineeringConfigTable;
    private KeyValueTable pipelineDataSchemasTable;
    private KeyValueTable pipelineNameTable;

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
        this.pipelineNameTableName = config.getPipelineNameTable();

        createDefaultFilter();
    }

    private void createDefaultFilter() {
        defaultFilter = new StatsFilter();
        defaultFilter.setFilterType(StatsFilterType.TopN);
        defaultFilter.setStatsName(FeatureSTATS.Feature);
        defaultFilter.setLowerLimit(0);
        defaultFilter.setUpperLimit(Integer.MAX_VALUE);
    }

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
        super.initialize(context);
        this.dataSchemaTable = context.getDataset(dataSchemaTableName);
        this.pluginConfigTable = context.getDataset(pluginConfigTableName);
        this.featureDAGTable = context.getDataset(featureDAGTableName);
        this.featureEngineeringConfigTable = context.getDataset(featureEngineeringConfigTableName);
        this.pipelineDataSchemasTable = context.getDataset(pipelineDataSchemasTableName);
        this.pipelineNameTable = context.getDataset(pipelineNameTableName);
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
            pipelineNameTable.write(featureSelectionRequest.getFeatureSelectionPipeline(),
                    PipelineType.FEATURE_SELECTION.getName());
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
                if (stat.equals(FeatureSTATS.Feature)) {
                    continue;
                }
                Object value = getStatColumnValue(stat, row);
                featureStat.addFeatureStat(stat.getName(), value);
            }
            featureStatsObj.addFeatureStat(featureStat);
        }
        responder.sendJson(featureStatsObj);
    }

    @POST
    @Path("featureengineering/{pipelineName}/features/filter")
    public void filterFeaturesByStats(HttpServiceRequest request, HttpServiceResponder responder,
            @PathParam("pipelineName") String featureGenerationPipelineName) {
        FilterFeaturesByStatsRequest filterFeaturesByStatsRequest = new RequestExtractor(request).getContent("UTF-8",
                FilterFeaturesByStatsRequest.class);
        Table featureStatsDataSet = context.getDataset(featureGenerationPipelineName);
        Scanner allFeatures = featureStatsDataSet.scan(null, null);
        Row row;
        Set<FeatureSTATS> featureStatSet = getFeatureStatsSet(filterFeaturesByStatsRequest);
        List<Queue<FeatureStatsPriorityNode>> queueList = new LinkedList<Queue<FeatureStatsPriorityNode>>();
        int rowIndex = 0;
        while ((row = allFeatures.next()) != null) {
            FeatureStats featureStat = new FeatureStats();
            String featureName = row.getString(FeatureSTATS.Feature.getName());
            featureStat.setFeatureName(featureName);
            for (FeatureSTATS stat : featureStatSet) {
                Object value = getStatColumnValue(stat, row);
                featureStat.addFeatureStat(stat.getName(), value);
            }
            prepareAllPriorityQueuesForTopFilters(rowIndex, filterFeaturesByStatsRequest, queueList, featureStat, row);
            rowIndex++;
        }
        List<FeatureStats> featureStatsList = combineAllPriorityQueues(queueList, filterFeaturesByStatsRequest);
        featureStatsList = applyRangeFiltersOnFeatureStats(filterFeaturesByStatsRequest, featureStatsList);
        featureStatsList = applyOrderByClause(featureStatsList, filterFeaturesByStatsRequest);
        featureStatsList = applyLimit(featureStatsList, filterFeaturesByStatsRequest);
        SelectedFeatureStats featureStatsObj = new SelectedFeatureStats();
        featureStatsObj.setFeatureStatsList(featureStatsList);
        responder.sendJson(featureStatsObj);
    }

    private List<FeatureStats> applyLimit(final List<FeatureStats> featureStatsList,
            final FilterFeaturesByStatsRequest filterFeaturesByStatsRequest) {
        List<FeatureStats> resultFeatureStatsList = new LinkedList<>();
        int startLimit = filterFeaturesByStatsRequest.getStartPosition();
        int endLimit = filterFeaturesByStatsRequest.getEndPosition();
        for (int index = startLimit; index <= endLimit && index < featureStatsList.size(); index++) {
            resultFeatureStatsList.add(featureStatsList.get(index));
        }
        return resultFeatureStatsList;
    }

    private List<FeatureStats> applyOrderByClause(List<FeatureStats> featureStatsList,
            FilterFeaturesByStatsRequest filterFeaturesByStatsRequest) {
        if (filterFeaturesByStatsRequest.getOrderByStat() == null) {
            return featureStatsList;
        }
        Collections.sort(featureStatsList, new Comparator<FeatureStats>() {

            @Override
            public int compare(FeatureStats entry1, FeatureStats entry2) {
                FeatureSTATS orderByStat = filterFeaturesByStatsRequest.getOrderByStat();
                Object value1 = entry1.getFeatureStatisticValue(orderByStat.getName());
                Object value2 = entry2.getFeatureStatisticValue(orderByStat.getName());
                return orderByStat.compare(value1, value2);
            }
        });
        return featureStatsList;
    }

    private List<FeatureStats> applyRangeFiltersOnFeatureStats(
            FilterFeaturesByStatsRequest filterFeaturesByStatsRequest, List<FeatureStats> featureStatsList) {
        List<FeatureStats> resultFeatureStatsList = new ArrayList<>();
        boolean union = true;
        if (filterFeaturesByStatsRequest.isComposite() && filterFeaturesByStatsRequest.getCompositeType() != null
                && filterFeaturesByStatsRequest.getCompositeType().equals(CompositeType.AND)) {
            union = false;
        }
        int totalRangeFilters = 0;
        for (StatsFilter filter : filterFeaturesByStatsRequest.getFilterList()) {
            if (filter.getFilterType().equals(StatsFilterType.Range)) {
                totalRangeFilters++;
            }
        }
        if (totalRangeFilters == 0) {
            return featureStatsList;
        }
        for (FeatureStats featureStat : featureStatsList) {
            int matchingCount = 0;
            for (StatsFilter filter : filterFeaturesByStatsRequest.getFilterList()) {
                if (filter.getFilterType().equals(StatsFilterType.Range)) {
                    StatsFilter rangeLimitFilter = (StatsFilter) filter;
                    if (satisfyRangeFilter(rangeLimitFilter, featureStat)) {
                        matchingCount++;
                    }
                }
            }
            if (union && matchingCount > 0) {
                resultFeatureStatsList.add(featureStat);
            } else if (!union && matchingCount == totalRangeFilters) {
                resultFeatureStatsList.add(featureStat);
            }
        }
        return resultFeatureStatsList;
    }

    private boolean satisfyRangeFilter(StatsFilter rangeLimitFilter, FeatureStats featureStat) {
        FeatureSTATS stat = rangeLimitFilter.getStatsName();
        Object statValue = featureStat.getFeatureStatisticValue(stat.getName());
        return rangeLimitFilter.getStatsName().liesInBetween(statValue, rangeLimitFilter.getLowerLimit(),
                rangeLimitFilter.getUpperLimit());
    }

    private List<FeatureStats> combineAllPriorityQueues(final List<Queue<FeatureStatsPriorityNode>> queueList,
            FilterFeaturesByStatsRequest filterFeaturesByStatsRequest) {
        boolean union = true;
        if (filterFeaturesByStatsRequest.isComposite() && filterFeaturesByStatsRequest.getCompositeType() != null
                && filterFeaturesByStatsRequest.getCompositeType().equals(CompositeType.AND)) {
            union = false;
        }
        Map<String, Long> featureOccurenceCountMap = new HashMap<String, Long>();
        Map<String, FeatureStats> featureStatsMap = new HashMap<String, FeatureStats>();
        for (Queue<FeatureStatsPriorityNode> queue : queueList) {
            FeatureStatsPriorityNode priorityNode = null;
            while ((priorityNode = queue.poll()) != null) {
                String featureName = priorityNode.getFeatureStats().getFeatureName();
                if (!featureOccurenceCountMap.containsKey(featureName)) {
                    featureOccurenceCountMap.put(featureName, 0L);
                }
                long count = featureOccurenceCountMap.get(featureName);
                featureOccurenceCountMap.put(featureName, count + 1);
                featureStatsMap.put(featureName, priorityNode.getFeatureStats());
            }
        }

        List<FeatureStats> featureStatsList = new ArrayList<>();
        if (union) {
            featureStatsList.addAll(featureStatsMap.values());
        } else {
            for (Map.Entry<String, Long> entry : featureOccurenceCountMap.entrySet()) {
                if (entry.getValue().equals(new Long(queueList.size()))
                        && featureStatsMap.containsKey(entry.getKey())) {
                    featureStatsList.add(featureStatsMap.get(entry.getKey()));
                }
            }
        }
        return featureStatsList;
    }

    private void prepareAllPriorityQueuesForTopFilters(int rowIndex,
            FilterFeaturesByStatsRequest filterFeaturesByStatsRequest, List<Queue<FeatureStatsPriorityNode>> queueList,
            FeatureStats featureStat, Row row) {
        int queueIndex = 0;
        if (filterFeaturesByStatsRequest.getFilterList() == null
                || filterFeaturesByStatsRequest.getFilterList().isEmpty()) {
            if (defaultFilter == null) {
                createDefaultFilter();
            }
            addPriorityNodeInQueue(defaultFilter, featureStat, row, queueList, rowIndex, queueIndex);
        }
        for (StatsFilter filter : filterFeaturesByStatsRequest.getFilterList()) {
            if (!filter.getFilterType().equals(StatsFilterType.Range)) {
                addPriorityNodeInQueue(filter, featureStat, row, queueList, rowIndex, queueIndex);
                queueIndex++;
            }
        }
    }

    private void addPriorityNodeInQueue(StatsFilter filter, FeatureStats featureStat, Row row,
            List<Queue<FeatureStatsPriorityNode>> queueList, int rowIndex, int queueIndex) {
        FeatureStatsPriorityNode priorityNode = getPriorityNode(filter, featureStat, row);
        if (rowIndex == 0) {
            queueList.add(new PriorityQueue<FeatureStatsPriorityNode>());
        }
        PriorityQueue queue = (PriorityQueue) queueList.get(queueIndex);
        queue.add(priorityNode);
        while (queue.size() > getIntValue(filter.getUpperLimit())) {
            queue.poll();
        }
    }

    private Integer getIntValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Double) {
            return ((Double) value).intValue();
        } else if (value instanceof Long) {
            return ((Long) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        return (int) value;
    }

    private FeatureStatsPriorityNode getPriorityNode(final StatsFilter singleLimitFilter, FeatureStats featureStat,
            Row row) {
        FeatureSTATS stat = singleLimitFilter.getStatsName();
        Object value = getStatColumnValue(stat, row);
        boolean isAscending = true;
        if (singleLimitFilter.getFilterType().equals(StatsFilterType.LowN)) {
            isAscending = false;
        }
        switch (stat.getType()) {
        case "long":
            return new FeatureStatsPriorityNode<Long>((Long) value, featureStat, isAscending);
        case "double":
            return new FeatureStatsPriorityNode<Double>((Double) value, featureStat, isAscending);
        case "string":
            return new FeatureStatsPriorityNode<String>((String) value, featureStat, isAscending);
        case "boolean":
            return new FeatureStatsPriorityNode<Boolean>((Boolean) value, featureStat, isAscending);
        }
        return null;
    }

    private Set<FeatureSTATS> getFeatureStatsSet(FilterFeaturesByStatsRequest filterFeaturesByStatsRequest) {
        Set<FeatureSTATS> featureStatSet = new HashSet<FeatureSTATS>();
        if (filterFeaturesByStatsRequest.getOrderByStat() != null) {
            featureStatSet.add(filterFeaturesByStatsRequest.getOrderByStat());
        }
        if (filterFeaturesByStatsRequest.getFilterList() != null) {
            for (StatsFilter filter : filterFeaturesByStatsRequest.getFilterList()) {
                featureStatSet.add(filter.getStatsName());
            }
        }
        if (filterFeaturesByStatsRequest.getFilterList() == null
                || filterFeaturesByStatsRequest.getFilterList().isEmpty()) {
            for (FeatureSTATS stat : FeatureSTATS.values()) {
                featureStatSet.add(stat);
            }
        }
        return featureStatSet;
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
