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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import co.cask.cdap.featureengineer.pipeline.pojo.*;
import co.cask.cdap.featureengineer.request.pojo.Relation;
import co.cask.cdap.featureengineer.request.pojo.SchemaColumn;

/**
 * @author bhupesh.goel
 *
 */
public class FeatureToolCodeGenerator {

//	public String generateFeatureToolCode(List<String> entityNames, List<NullableSchema> dataSchemaList,
//			List<SchemaColumn> indexes, List<Relation> relationShips, List<String> aggregatePrimitives,
//			List<String> transformPrimitives, Integer dfsDepth, String targetEntity,
//			List<SchemaColumn> timestampColumns, List<SchemaColumn> categoricalColumns,
//			List<SchemaColumn> ignoreColumns, Map<String, Map<String, List<String>>> appliedTransFunctionsWithArguments,
//			Map<String, Map<String, List<String>>> appliedAggFunctionsWithArguments) {
//		return null;
//	}

	public String generateFeatureToolCode(final List<String> dataSourceName, final List<NullableSchema> dataSchema,
			List<SchemaColumn> indexes, List<Relation> relations, List<String> aggPrimitives,
			List<String> transPrimitives, int dfsDepth, String targetEntity, List<SchemaColumn> timestampColumnList,
			List<SchemaColumn> categoricalColumns, List<SchemaColumn> ignoreColumns,
			Map<String, Map<String, List<String>>> appliedTransFunctionsWithArguments,
			Map<String, Map<String, List<String>>> appliedAggFunctionsWithArguments) {
		Set<SchemaColumn> timestampColumns = new HashSet<SchemaColumn>(timestampColumnList);
		Map<String, List<String>> entityStringColumnMap = new HashMap<String, List<String>>();
		StringBuilder sb = new StringBuilder();
		sb.append("from builtins import range\n"
				+ "from featuretools.primitives import make_trans_primitive,make_agg_primitive\n"
				+ "from featuretools.variable_types import (Boolean,Datetime,DatetimeTimeIndex,Discrete,Id,LatLong,Numeric,Ordinal,Text,Timedelta,Variable)\n"
				+ "import pandas as pd\n" + "from numpy import random\n" + "from numpy.random import choice\n"
				+ "from pandas import DatetimeIndex\n" + "import featuretools as ft\n"
				+ "from featuretools.variable_types import Categorical\n" + "import numpy as np\n"
				+ "from collections import defaultdict\n\n");
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
				if (k < schema.getFields().length - 1)
					sb.append(",");
				k++;
			}
			addFunctionColumns(appliedTransFunctionsWithArguments, dataSourceName.get(i), sb, entityStringColumnMap,
					timestampColumns);
			StringBuilder sbLocal = tableStringBuilderMap.get(dataSourceName.get(i));
			if (sbLocal != null)
				sb.append(sbLocal.toString());
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
				if (tableCategoricalColumnMap.containsKey(dataSourceName.get(i)))
					columnSet = tableCategoricalColumnMap.get(dataSourceName.get(i));

				int j = 0;
				if (columnSet != null) {
					for (String col : columnSet) {
						if (j > 0)
							sb.append(",");
						sb.append("\"" + col + "\": ft.variable_types.Categorical");
						j++;
					}
				}

				List<String> columnList = entityStringColumnMap.get(dataSourceName.get(i));
				if (columnList != null) {
					for (String col : columnList) {
						if (columnSet.contains(col))
							continue;
						if (j > 0)
							sb.append(",");
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

		addIgnoreVariablesCode(ignoreColumns, sb);
		sb.append("feature_defs, all_feature, feature_tree = ft.dfs(entityset=es,target_entity=\"");
		sb.append(targetEntity + "\",agg_primitives=[");
		for (int i = 0; i < aggPrimitives.size(); i++) {
			if (i > 0)
				sb.append(",");
			sb.append("\"" + aggPrimitives.get(i) + "\"");
		}
		sb.append("],trans_primitives=[");
		for (int i = 0; i < transPrimitives.size(); i++) {
			if (i > 0)
				sb.append(",");
			sb.append("\"" + transPrimitives.get(i) + "\"");
		}
		sb.append("],max_depth=" + dfsDepth);
		if (ignoreColumns != null && !ignoreColumns.isEmpty()) {
			sb.append(", ignore_variables=ignore_variables)");
		} else
			sb.append(")");
		// sb.append("\nfeature_tree.table_feature_operations_mapping_dag\n");

		return sb.toString();
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
		if (columnType == null)
			return;
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
		if (ignoreColumns == null || ignoreColumns.isEmpty())
			return;
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
		if (categoricalColumns == null)
			return tableCategoricalColumnMap;
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
