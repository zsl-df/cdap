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

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.featureengineer.FeatureEngineeringApp.FeatureEngineeringConfig;
import co.cask.cdap.featureengineer.RequestExtractor;
import co.cask.cdap.featureengineer.exception.DataSchemaReadException;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchema;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchemaField;
import co.cask.cdap.featureengineer.proto.PersistWranglerRequest;
import co.cask.cdap.featureengineer.request.pojo.Column;
import co.cask.cdap.featureengineer.request.pojo.DataSchema;
import co.cask.cdap.featureengineer.request.pojo.DataSchemaList;
import co.cask.cdap.featureengineer.utils.JSONInputParser;

/**
 * @author bhupesh.goel
 *
 */
public class DataPrepSchemaServiceHandler extends BaseServiceHandler {

	private static final Logger LOG = LoggerFactory.getLogger(DataPrepSchemaServiceHandler.class);

	@Property
	private final String dataSchemaTableName;
	@Property
	private final String pluginConfigTableName;

	private KeyValueTable dataSchemaTable;
	private KeyValueTable pluginConfigTable;

	/**
	 * @param config
	 * 
	 */
	public DataPrepSchemaServiceHandler(FeatureEngineeringConfig config) {
		this.dataSchemaTableName = config.getDataSchemaTable();
		this.pluginConfigTableName = config.getPluginConfigTable();
	}

	@Override
	public void initialize(HttpServiceContext context) throws Exception {
		super.initialize(context);
		dataSchemaTable = context.getDataset(dataSchemaTableName);
		pluginConfigTable = context.getDataset(pluginConfigTableName);
	}

	@POST
	@Path("featureengineering/{dataSchemaName}/wrangler/{configType}/config")
	public void persistWranglerPluginConfig(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("dataSchemaName") String dataSchemaName, @PathParam("configType") String configType) {
		try {
			PersistWranglerRequest wranglerRequest = new RequestExtractor(request).getContent("UTF-8",
					PersistWranglerRequest.class);
			LOG.debug("Passed pluginConfig is " + wranglerRequest.getPluginConfig());
			NullableSchema schema = JSONInputParser.parseDataSchemaJSON(wranglerRequest.getSchema());
			if (schema.getFields().length == 1) {
				dataSchemaName = "accounts";
			} else {
				dataSchemaName = "errors";
			}
			dataSchemaTable.write(dataSchemaName, wranglerRequest.getSchema());
			pluginConfigTable.write(dataSchemaName + "_" + configType, wranglerRequest.getPluginConfig());
			success(responder, "Successfully persisted wrangler plugin config");
		} catch (Exception e) {
			error(responder, e.getMessage());
		}
	}

	@GET
	@Path("featureengineering/dataschema/getall")
	public void getAllDataSchemas(HttpServiceRequest request, HttpServiceResponder responder) {
		CloseableIterator<KeyValue<byte[], byte[]>> iterator = dataSchemaTable.scan(null, null);
		DataSchemaList schemaList = new DataSchemaList();
		while (iterator.hasNext()) {
			KeyValue<byte[], byte[]> keyValue = iterator.next();
			String schemaName = Bytes.toString(keyValue.getKey());
			String schema = Bytes.toString(keyValue.getValue());
			NullableSchema nullableSchema = JSONInputParser.parseDataSchemaJSON(schema);
			DataSchema dataSchema = createDataSchemaFromNullableSchema(nullableSchema, schemaName);
			schemaList.addDataSchema(dataSchema);
		}
		responder.sendJson(schemaList);
	}

	private DataSchema createDataSchemaFromNullableSchema(NullableSchema nullableSchema, String schemaName) {
		DataSchema dataSchema = new DataSchema();
		dataSchema.setSchemaName(schemaName);
		for (NullableSchemaField field : nullableSchema.getFields()) {
			Column column = new Column();
			column.setColumnName(field.getName());
			try {
				column.setColumnType(field.getType().get(0));
			} catch (Throwable th) {
				throw new DataSchemaReadException(
						"Unable to read Column Type for column " + field.getName() + " of schema " + schemaName + " ");
			}
			
			dataSchema.addSchemaColumn(column);
		}
		return dataSchema;
	}
}
