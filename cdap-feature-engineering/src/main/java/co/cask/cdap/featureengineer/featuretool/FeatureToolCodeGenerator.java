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
package co.cask.cdap.featureengineer.featuretool;

import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchema;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchemaField;
import co.cask.cdap.featureengineer.pipeline.pojo.Schema;
import co.cask.cdap.featureengineer.pipeline.pojo.SchemaField;
import co.cask.cdap.featureengineer.pipeline.pojo.SchemaFieldName;
import co.cask.cdap.featureengineer.request.pojo.Relation;
import co.cask.cdap.featureengineer.request.pojo.SchemaColumn;
import co.cask.cdap.proto.artifact.PluginSummary;

import java.util.Arrays;
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
public class FeatureToolCodeGenerator {

    private enum FeatureToolsTransformPrimitives {
        IS_NULL("is_null"), 
        LOG("log"), 
        ABSOLUTE("absolute"), 
        TIME_SINCE_PREVIOUS("time_since_previous"), 
        DAY("day"), 
        DAYS("days"), 
        HOUR("hour"), 
        HOURS("hours"), 
        SECOND("second"), 
        SECONDS("seconds"), 
        MINUTE("minute"), 
        MINUTES("minutes"), 
        WEEK("week"), 
        WEEKS("weeks"), 
        MONTH("month"), 
        MONTHS("months"), 
        YEAR("year"), 
        YEARS("years"), 
        WEEKEND("weekend"), 
        WEEKDAY("weekday"), 
        NUMCHARACTERS("numcharacters"), 
        NUMWORDS("numwords"), 
        DAYS_SINCE("days_since"), 
        ISIN("isin"), 
        DIFF("diff"), 
        NOT("not"), 
        PERCENTILE("percentile"), 
        LATITUDE("latitude"), 
        LONGITUDE("longitude"), 
        HAVERSINE("haversine");

        private String name;

        FeatureToolsTransformPrimitives(final String name) {
            this.name = name;
        }
    }

    private enum FeatureToolsAggregatePrimitives {
        COUNT("count"), 
        SUM("sum"), 
        VARIANCE("variance"), 
        AVG("avg"), 
        MODE("mode"), 
        MIN("min"), 
        MAX("max"), 
        N_UNIQ("nuniq"), 
        VALUE_COUNT("valuecount"), 
        INDICATOR_COUNT("indicatorcount"), 
        NUM_TRUE("num_true"), 
        PERCENT_TRUE("percent_true"), 
        N_MOST_COMMON("n_most_common"), 
        AVG_TIME_BETWEEN("avg_time_between"), 
        MEDIAN("median"), 
        SKEW("skew"), 
        STD_DEV("stddev"), 
        LAST("last"), 
        FIRST("first"), 
        ANY("any"), 
        ALL("all"), 
        TIME_SINCE_LAST("time_since_last"), 
        TREND("trend");

        private String name;

        FeatureToolsAggregatePrimitives(final String name) {
            this.name = name;
        }
    }

    public String generateFeatureToolCode(final List<String> dataSourceName, final List<NullableSchema> dataSchema,
            List<SchemaColumn> indexes, List<Relation> relations, List<String> aggPrimitives,
            List<String> transPrimitives, int dfsDepth, String targetEntity, List<SchemaColumn> timestampColumnList,
            List<SchemaColumn> categoricalColumns, List<SchemaColumn> ignoreColumns,
            Map<String, Map<String, List<String>>> appliedTransFunctionsWithArguments,
            Map<String, Map<String, List<String>>> appliedAggFunctionsWithArguments,
            Map<String, PluginSummary> transformPluginFunctionMap,
            Map<String, PluginSummary> aggregatePluginFunctionMap) {
        Set<SchemaColumn> timestampColumns = new HashSet<SchemaColumn>(timestampColumnList);
        Map<String, List<String>> entityStringColumnMap = new HashMap<String, List<String>>();
        StringBuilder sb = new StringBuilder();
        sb.append("from builtins import range\n"
                + "from featuretools.primitives import make_trans_primitive,make_agg_primitive\n"
                + "from featuretools.variable_types import (Boolean,Datetime,DatetimeTimeIndex,Discrete,Id,LatLong,"
                + "Numeric,Ordinal,Text,Timedelta,Variable)\n" + "import pandas as pd\n" + "from numpy import random\n"
                + "from numpy.random import choice\n" + "from pandas import DatetimeIndex\n"
                + "import featuretools as ft\n" + "from featuretools.variable_types import Categorical\n"
                + "import numpy as np\n" + "from collections import defaultdict\n\n");
        Map<String, StringBuilder> tableStringBuilderMap = addAggFunctionColumns(appliedAggFunctionsWithArguments,
                entityStringColumnMap, timestampColumns);

        for (int i = 0; i < dataSourceName.size(); i++) {
            sb.append(dataSourceName.get(i) + "_df");
            sb.append(" = pd.DataFrame(index=[''],data={");
            NullableSchema schema = dataSchema.get(i);
            int k = 0;
            for (SchemaFieldName field : schema.getFields()) {
                sb.append("'" + field.getName() + "' : np.full(1, ");
                sb.append(getTypeString(field, dataSourceName.get(i), entityStringColumnMap, timestampColumns) + ")");
                if (k < schema.getFields().length - 1) {
                    sb.append(",");
                }
                k++;
            }
            addFunctionColumns(appliedTransFunctionsWithArguments, dataSourceName.get(i), sb, entityStringColumnMap,
                    timestampColumns);
            StringBuilder sbLocal = tableStringBuilderMap.get(dataSourceName.get(i));
            if (sbLocal != null) {
                sb.append(sbLocal.toString());
            }
            sb.append("})\n");
        }
        Map<String, Set<String>> tableCategoricalColumnMap = breakCategoricalColumnsTableWise(categoricalColumns);
        sb.append("es = ft.EntitySet(id=\"entityset\")\n");
        for (int i = 0; i < dataSourceName.size(); i++) {
            String indexColumn = indexes.get(i).getColumn();
            sb.append("es = es.entity_from_dataframe(entity_id=\"" + dataSourceName.get(i) + "\",dataframe="
                    + dataSourceName.get(i) + "_df,index=\"" + indexColumn + "\"");

            if (entityStringColumnMap.containsKey(dataSourceName.get(i))
                    || tableCategoricalColumnMap.containsKey(dataSourceName.get(i))) {
                sb.append(",variable_types={");

                Set<String> columnSet = new HashSet<String>();
                if (tableCategoricalColumnMap.containsKey(dataSourceName.get(i))) {
                    columnSet = tableCategoricalColumnMap.get(dataSourceName.get(i));
                }
                int j = 0;
                if (columnSet != null) {
                    for (String col : columnSet) {
                        if (j > 0) {
                            sb.append(",");
                        }
                        sb.append("\"" + col + "\": ft.variable_types.Categorical");
                        j++;
                    }
                }

                List<String> columnList = entityStringColumnMap.get(dataSourceName.get(i));
                if (columnList != null) {
                    for (String col : columnList) {
                        if (columnSet.contains(col)) {
                            continue;
                        }
                        if (j > 0) {
                            sb.append(",");
                        }
                        sb.append("\"" + col + "\": ft.variable_types.Text");
                        j++;
                    }
                }
                sb.append("}");
            }
            sb.append(")\n");
        }
        int index = 0;
        if (relations.size() > 0) {
            sb.append("rels = [");
        }
        for (Relation relation : relations) {
            if (index > 0) {
                sb.append(",");
            }
            sb.append("ft.Relationship(es[\"" + relation.getColumn1().getTable() + "\"][\""
                    + relation.getColumn1().getColumn() + "\"],es[\"" + relation.getColumn2().getTable() + "\"][\""
                    + relation.getColumn2().getColumn() + "\"])");
            index++;
        }
        sb.append("]\nes = es.add_relationships(rels)\n");

        List<String> dynamicTransPrimitives = new LinkedList<String>();
        List<String> dynamicAggPrimitives = new LinkedList<String>();

        addCodeToLoadNewPrimitivesDynamically(transformPluginFunctionMap, aggregatePluginFunctionMap, sb,
                dynamicTransPrimitives, dynamicAggPrimitives, transPrimitives, aggPrimitives);
        addIgnoreVariablesCode(ignoreColumns, sb);
        sb.append("feature_defs, all_feature, feature_tree = ft.dfs(entityset=es,target_entity=\"");
        sb.append(targetEntity + "\",agg_primitives=[");
        index = 0;
        for (index = 0; index < aggPrimitives.size(); index++) {
            if (index > 0) {
                sb.append(",");
            }
            sb.append("\"" + aggPrimitives.get(index) + "\"");
        }
        for (String primitive : dynamicAggPrimitives) {
            if (index > 0) {
                sb.append(",");
            }
            sb.append(primitive);
        }
        sb.append("],trans_primitives=[");
        for (index = 0; index < transPrimitives.size(); index++) {
            if (index > 0) {
                sb.append(",");
            }
            sb.append("\"" + transPrimitives.get(index) + "\"");
        }
        for (String primitive : dynamicTransPrimitives) {
            if (index > 0) {
                sb.append(",");
            }
            sb.append(primitive);
        }
        sb.append("],max_depth=" + dfsDepth);
        if (ignoreColumns != null && !ignoreColumns.isEmpty()) {
            sb.append(", ignore_variables=ignore_variables)");
        } else {
            sb.append(")");
        }
        return sb.toString();
    }

    private void addCodeToLoadNewPrimitivesDynamically(final Map<String, PluginSummary> transformPluginFunctionMap,
            final Map<String, PluginSummary> aggregatePluginFunctionMap, StringBuilder sb,
            List<String> dynamicTransPrimitives, List<String> dynamicAggPrimitives, List<String> transPrimitives,
            List<String> aggPrimitives) {
        Map<String, List<String>> transformFunctionIOMap = new HashMap<>();
        Map<String, List<String>> aggregationFunctionIOMap = new HashMap<>();
        Map<String, PluginSummary> transformPluginFunctionMapCopy = new HashMap<>(transformPluginFunctionMap);
        Map<String, PluginSummary> aggregatePluginFunctionMapCopy = new HashMap<>(aggregatePluginFunctionMap);
        transPrimitives.clear();
        for (FeatureToolsTransformPrimitives primitive : FeatureToolsTransformPrimitives.values()) {
            transPrimitives.add(primitive.name);
            if (transformPluginFunctionMapCopy.containsKey(primitive.name)) {
                transformPluginFunctionMapCopy.remove(primitive.name);
            } else {
                transPrimitives.remove(primitive.name);
            }
        }
        aggPrimitives.clear();
        for (FeatureToolsAggregatePrimitives primitive : FeatureToolsAggregatePrimitives.values()) {
            aggPrimitives.add(primitive.name);
            if (aggregatePluginFunctionMapCopy.containsKey(primitive.name)) {
                aggregatePluginFunctionMapCopy.remove(primitive.name);
            } else {
                aggPrimitives.remove(primitive.name);
            }
        }
        generateDynamicPrimitivesCode(transformPluginFunctionMapCopy, dynamicTransPrimitives, sb,
                "make_trans_primitive");
        generateDynamicPrimitivesCode(aggregatePluginFunctionMapCopy, dynamicAggPrimitives, sb, "make_agg_primitive");

    }

    private void generateDynamicPrimitivesCode(Map<String, PluginSummary> pluginFunctionMapCopy,
            List<String> dynamicPrimitives, StringBuilder sb, String primitiveFunction) {
        Set<String> visitedPrimitive = new HashSet<>();
        for (PluginSummary plugin : pluginFunctionMapCopy.values()) {
            String[] functions = plugin.getPluginFunction();
            String[] inputTypes = plugin.getPluginInput();
            String[] outputTypes = plugin.getPluginOutput();
            for (int i = 0; i < functions.length; i++) {
                if (!pluginFunctionMapCopy.containsKey(functions[i])) {
                    continue;
                }
                if (visitedPrimitive.contains(functions[i].toLowerCase())) {
                    continue;
                }
                visitedPrimitive.add(functions[i].toLowerCase());
                String featureToolsInputDataType = getFeatureToolsDataType(inputTypes[i]);
                String featureToolsOutputDataType = getFeatureToolsDataType(outputTypes[i]);
                if (featureToolsOutputDataType == null) {
                    featureToolsOutputDataType = featureToolsInputDataType;
                }
                sb.append("\ndef pd_" + functions[i].toLowerCase() + "():\n\treturn \"\"\n\n");
                sb.append("def " + functions[i].toLowerCase() + "_generate_name(self):\n\treturn u\""
                        + functions[i].toUpperCase() + "(%s)\" % (self.base_features[0].get_name())\n\n");
                sb.append("def get_" + functions[i].toLowerCase() + "_function_name(self):\n\treturn u\""
                        + functions[i].toUpperCase() + "\"\n\n");
                String dynamicFuncName = "pd_" + functions[i] + "_trans";
                sb.append(dynamicFuncName + " = " + primitiveFunction + "(function=pd_" + functions[i].toLowerCase());
                sb.append(",input_types=[" + featureToolsInputDataType + "],return_type=" + featureToolsOutputDataType);
                sb.append(",name=\"" + functions[i] + "\",description=\"" + plugin.getDescription()
                        + "\",cls_attributes={\"generate_name\": ");
                sb.append(functions[i].toLowerCase() + "_generate_name,\"get_function_name\": get_"
                        + functions[i].toLowerCase() + "_function_name})\n");
                dynamicPrimitives.add(dynamicFuncName);
            }
        }
    }

    private String getFeatureToolsDataType(String cdapType) {
        if (cdapType == null || cdapType.isEmpty()) {
            return "";
        }
        cdapType = cdapType.trim().toLowerCase();
        if (cdapType.contains(" ")) {
            StringBuilder sb = new StringBuilder();
            int index = 0;
            for (String curType : cdapType.split("\\s+")) {
                if (index > 0) {
                    sb.append(" ,");
                }
                sb.append(getFeatureToolsDataType(curType));
            }
            return sb.toString();
        }
        if (cdapType.startsWith("list<") && cdapType.endsWith(">")) {
            return getFeatureToolsDataType(cdapType.trim().substring(5, cdapType.length() - 1));
        }
        if (isAlphaBeticWord(cdapType)) {
            return getFTSingleDataType(cdapType);
        } else {
            return getFTCombinedDataType(cdapType);
        }
    }

    private String getFTCombinedDataType(String cdapType) {
        String cdapSingleType = cdapType.split("\\s+")[0];
        if (cdapSingleType.equals("*")) {
            return "Variable";
        }
        String[] types = cdapSingleType.split(":");
        boolean isIntegral = false;
        boolean isDecimal = false;
        boolean isText = false;
        boolean isBoolean = false;
        for (String type : types) {
            switch (type) {
            case "int":
            case "long":
                isIntegral = true;
                break;
            case "double":
            case "float":
                isDecimal = true;
                break;
            case "string":
                isText = true;
                break;
            case "boolean":
                isBoolean = true;
                break;
            }
        }
        if ((isIntegral || isDecimal) && isBoolean && isText) {
            return "Variable";
        } else if (isText && (isIntegral || isBoolean) && !isDecimal) {
            return "Discrete";
        } else if ((isIntegral || isDecimal) && !isBoolean && !isText) {
            return "Numeric";
        } else if (isBoolean && !isIntegral && !isDecimal && !isText) {
            return "Boolean";
        } else if (!isBoolean && !isIntegral && !isDecimal && isText) {
            return "Text";
        }
        return "Variable";
    }

    private String getFTSingleDataType(String cdapType) {
        switch (cdapType) {
        case "int":
        case "long":
        case "float":
        case "double":
            return "Numeric";
        case "string":
            return "Text";
        case "boolean":
            return "Boolean";
        case "datetime":
            return "Datetime";
        }
        return null;
    }

    private boolean isAlphaBeticWord(final String cdapType) {
        for (int i = 0; i < cdapType.length(); i++) {
            if (!Character.isAlphabetic(cdapType.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private Map<String, StringBuilder> addAggFunctionColumns(
            Map<String, Map<String, List<String>>> appliedAggFunctionsWithArguments,
            Map<String, List<String>> entityStringColumnMap, Set<SchemaColumn> timestampColumns) {
        Map<String, StringBuilder> tableStringBuilderMap = new HashMap<>();
        for (Map.Entry<String, Map<String, List<String>>> entry : appliedAggFunctionsWithArguments.entrySet()) {
            for (Map.Entry<String, List<String>> entry2 : entry.getValue().entrySet()) {
                SchemaField field = new SchemaField();
                String tableName = entry2.getValue().get(4);
                StringBuilder sb = tableStringBuilderMap.get(tableName);
                if (sb == null) {
                    sb = new StringBuilder();
                    tableStringBuilderMap.put(tableName, sb);
                }
                field.setName(entry2.getKey());
                field.setType(entry2.getValue().get(2));
                sb.append(",");
                sb.append("'" + entry2.getKey() + "' : np.full(1, ");
                sb.append(getTypeString(field, tableName, entityStringColumnMap, timestampColumns) + ")");
            }
        }
        return tableStringBuilderMap;
    }

    private void addFunctionColumns(Map<String, Map<String, List<String>>> appliedFunctionsWithArguments,
            String tableName, StringBuilder sb, Map<String, List<String>> entityStringColumnMap,
            Set<SchemaColumn> timestampColumns) {
        Map<String, List<String>> columnType = appliedFunctionsWithArguments.get(tableName);
        if (columnType == null) {
            return;
        }
        for (Map.Entry<String, List<String>> entry : columnType.entrySet()) {
            SchemaField field = new SchemaField();
            field.setName(entry.getKey());
            field.setType(entry.getValue().get(2));
            sb.append(",");
            sb.append("'" + entry.getKey() + "' : np.full(1, ");
            sb.append(getTypeString(field, tableName, entityStringColumnMap, timestampColumns) + ")");
        }
    }

    private void addIgnoreVariablesCode(List<SchemaColumn> ignoreColumns, StringBuilder sb) {
        if (ignoreColumns == null || ignoreColumns.isEmpty()) {
            return;
        }
        sb.append("ignore_variables = defaultdict(set)\n");
        Set<String> initializedEntity = new HashSet<String>();

        for (SchemaColumn input : ignoreColumns) {
            if (!initializedEntity.contains(input.getTable())) {
                sb.append("ignore_variables[\"" + input.getTable() + "\"] = []\n");
                initializedEntity.add(input.getTable());
            }
            sb.append("ignore_variables[\"" + input.getTable() + "\"].append(\"" + input.getColumn() + "\")\n");
        }
    }

    private Map<String, Set<String>> breakCategoricalColumnsTableWise(List<SchemaColumn> categoricalColumns) {
        Map<String, Set<String>> tableCategoricalColumnMap = new HashMap<String, Set<String>>();
        if (categoricalColumns == null) {
            return tableCategoricalColumnMap;
        }
        for (SchemaColumn column : categoricalColumns) {
            Set<String> categoricalColumnList = tableCategoricalColumnMap.get(column.getTable());
            if (categoricalColumnList == null) {
                categoricalColumnList = new HashSet<String>();
                tableCategoricalColumnMap.put(column.getTable(), categoricalColumnList);
            }
            categoricalColumnList.add(column.getColumn());
        }
        return tableCategoricalColumnMap;
    }

    private String getTypeString(SchemaFieldName field, String entityName,
            Map<String, List<String>> entityStringColumnMap, Set<SchemaColumn> timestampColumns) {
        switch (getSchemaType(field)) {
        case "double":
        case "float":
            return "np.nan, dtype=float";
        case "long":
            if (timestampColumns.contains(new SchemaColumn(entityName, field.getName()))) {
                return "\"\", dtype=\"datetime64[ns]\"";
            }
            return "np.nan, dtype=long";
        case "int":
            return "np.nan, dtype=int";
        case "boolean":
            return "np.nan, dtype=bool";
        case "string":
            if (timestampColumns.contains(new SchemaColumn(entityName, field.getName()))) {
                return "\"\", dtype=\"datetime64[ns]\"";
            }
            List<String> columns = entityStringColumnMap.get(entityName);
            if (columns == null) {
                columns = new LinkedList<String>();
                entityStringColumnMap.put(entityName, columns);
            }
            columns.add(field.getName());
            return "\"\", dtype=str";
        default:
            if (getSchemaType(field).contains("time")) {
                return "\"\", dtype=\"datetime64[ns]\"";
            }
            break;
        }
        return "";
    }

    private static String getSchemaType(SchemaFieldName field) {
        if (field instanceof SchemaField) {
            return ((SchemaField) field).getType();
        } else if (field instanceof NullableSchemaField) {
            return ((NullableSchemaField) field).getType().get(0);
        }
        return null;
    }

    public static void main(String args[]) {
        List<Schema> schemaList = new LinkedList<Schema>();
        Schema schema = new Schema();
        schema.setName("name1");
        schema.setType("record");
        List<SchemaField> sfList = new LinkedList<SchemaField>();
        SchemaField sf = new SchemaField();
        sf.setName("customer_id");
        sf.setType("int");
        sfList.add(sf);
        sf = new SchemaField();
        sf.setName("join_date");
        sf.setType("long");
        sfList.add(sf);
        sf = new SchemaField();
        sf.setName("points");
        sf.setType("int");
        sfList.add(sf);
        sf = new SchemaField();
        sf.setName("zip_code");
        sf.setType("string");
        sfList.add(sf);
        schema.setFields((SchemaField[]) sfList.toArray(new SchemaField[0]));
        schemaList.add(schema);

        schema = new Schema();
        schema.setName("name2");
        schema.setType("record");
        sfList = new LinkedList<SchemaField>();
        sf = new SchemaField();
        sf.setName("product_id");
        sf.setType("int");
        sfList.add(sf);
        sf = new SchemaField();
        sf.setName("brand");
        sf.setType("string");
        sfList.add(sf);
        sf = new SchemaField();
        sf.setName("points");
        sf.setType("int");
        sfList.add(sf);
        schema.setFields((SchemaField[]) sfList.toArray(new SchemaField[0]));
        schemaList.add(schema);

        schema = new Schema();
        schema.setName("name3");
        schema.setType("record");
        sfList = new LinkedList<SchemaField>();
        sf = new SchemaField();
        sf.setName("session_id");
        sf.setType("int");
        sfList.add(sf);
        sf = new SchemaField();
        sf.setName("customer_id");
        sf.setType("int");
        sfList.add(sf);
        sf = new SchemaField();
        sf.setName("device");
        sf.setType("string");
        sfList.add(sf);
        sf = new SchemaField();
        sf.setName("session_start");
        sf.setType("long");
        sfList.add(sf);
        schema.setFields((SchemaField[]) sfList.toArray(new SchemaField[0]));
        schemaList.add(schema);

        schema = new Schema();
        schema.setName("name4");
        schema.setType("record");
        sfList = new LinkedList<SchemaField>();
        sf = new SchemaField();
        sf.setName("transaction_id");
        sf.setType("int");
        sfList.add(sf);
        sf = new SchemaField();
        sf.setName("amount");
        sf.setType("float");
        sfList.add(sf);
        sf = new SchemaField();
        sf.setName("transaction_time");
        sf.setType("long");
        sfList.add(sf);
        sf = new SchemaField();
        sf.setName("product_id");
        sf.setType("int");
        sfList.add(sf);
        sf = new SchemaField();
        sf.setName("session_id");
        sf.setType("int");
        sfList.add(sf);
        schema.setFields((SchemaField[]) sfList.toArray(new SchemaField[0]));
        schemaList.add(schema);

        List<String> indexes = new LinkedList<String>();
        indexes.add("customers.customer_id");
        indexes.add("products.product_id");
        indexes.add("session_df.session_id");
        indexes.add("transactions.transaction_id");

        List<String> relations = new LinkedList<String>();
        relations.add("products.product_id:transactions.product_id");
        relations.add("sessions.session_id:transactions.session_id");
        relations.add("customers.customer_id:sessions.customer_id");

        List<String> aggPrimitives = Arrays.asList("count", "min", "max");
        List<String> transPrimitives = Arrays.asList("month", "day", "weekday", "numwords", "characters");
        List<String> dataSourceName = Arrays.asList("customers", "products", "sessions", "transactions");
        Set<String> timestamp = new HashSet<String>(
                Arrays.asList("customers.join_date", "transactions.transaction_time", "sessions.session_start"));
        // new FeatureToolCodeGenerator().generateFeatureToolCode(dataSourceName,
        // schemaList, indexes, relations,
        // aggPrimitives, transPrimitives, 4, "customers", timestamp, null);
    }

}
