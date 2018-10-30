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

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonObject;

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.featureengineer.pipeline.pojo.CDAPPipelineInfo;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchema;
import co.cask.cdap.featureengineer.utils.JSONInputParser;

/**
 * @author bhupesh.goel
 *
 */
public class BaseServiceHandler extends AbstractHttpServiceHandler {
	
	/**
	 * Sends the error response back to client.
	 *
	 * @param responder
	 *            to respond to the service request.
	 * @param message
	 *            to be included as part of the error
	 */
	protected final void error(HttpServiceResponder responder, String message) {
		JsonObject error = new JsonObject();
		error.addProperty("status", HttpURLConnection.HTTP_INTERNAL_ERROR);
		error.addProperty("message", message);
		sendJson(responder, HttpURLConnection.HTTP_INTERNAL_ERROR, error.toString());
	}

	/**
	 * Sends the error response back to client with error status.
	 *
	 * @param responder
	 *            to respond to the service request.
	 * @param message
	 *            to be included as part of the error
	 */
	protected final void error(HttpServiceResponder responder, int status, String message) {
		JsonObject error = new JsonObject();
		error.addProperty("status", status);
		error.addProperty("message", message);
		sendJson(responder, HttpURLConnection.HTTP_NOT_FOUND, error.toString());
	}

	/**
	 * Returns a Json response back to client.
	 *
	 * @param responder
	 *            to respond to the service request.
	 * @param status
	 *            code to be returned to client.
	 * @param body
	 *            to be sent back to client.
	 */
	protected final void sendJson(HttpServiceResponder responder, int status, String body) {
		responder.send(status, ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8)), "application/json",
				new HashMap<String, String>());
	}

	/**
	 * Sends the success response back to client.
	 *
	 * @param responder
	 *            to respond to the service request.
	 * @param message
	 *            to be included as part of the error
	 */
	protected final void success(HttpServiceResponder responder, String message) {
		JsonObject error = new JsonObject();
		error.addProperty("status", HttpURLConnection.HTTP_OK);
		error.addProperty("message", message);
		sendJson(responder, HttpURLConnection.HTTP_OK, error.toString());
	}

	public void initialize(HttpServiceContext context) throws Exception {
		super.initialize(context);
	}
	
	protected String readCDAPKeyValueTable(final KeyValueTable table, final String key) {
		byte[] value = table.read(key);
		return new String(value, StandardCharsets.UTF_8);
	}
	
	protected String[] getHostAndPort(HttpServiceRequest request) {
		return request.getHeader("Host").split(":");
	}

	protected Map<String, CDAPPipelineInfo> getWranglerPluginConfigMap(List<String> dataSchemaNames, KeyValueTable pluginConfigTable) {
		Map<String, CDAPPipelineInfo> wranglerPluginConfigMap = new HashMap<String, CDAPPipelineInfo>();
		for (String schemaName : dataSchemaNames) {
			byte[] pluginConfigBytes = pluginConfigTable.read(schemaName + "_batch");
			String pluginConfig = new String(pluginConfigBytes, StandardCharsets.UTF_8);
			wranglerPluginConfigMap.put(schemaName, JSONInputParser.parseWranglerPlugin(pluginConfig));
		}
		return wranglerPluginConfigMap;
	}

	protected Map<String, NullableSchema> getSchemaMap(final List<String> dataSchemaNames, KeyValueTable dataSchemaTable) {
		Map<String, NullableSchema> schemaMap = new HashMap<String, NullableSchema>();
		for (String schemaName : dataSchemaNames) {
			byte[] schemaBytes = dataSchemaTable.read(schemaName);
			String schema = new String(schemaBytes, StandardCharsets.UTF_8);
			schemaMap.put(schemaName, JSONInputParser.parseDataSchemaJSON(schema));
		}
		return schemaMap;
	}

}
