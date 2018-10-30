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
package co.cask.cdap.featureengineer.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import co.cask.cdap.featureengineer.pipeline.pojo.CDAPPipelineInfo;
import co.cask.cdap.featureengineer.pipeline.pojo.NullableSchema;

/**
 * @author bhupesh.goel
 *
 */
public class JSONInputParser {

	private static final Gson gson = new GsonBuilder().create();

	private static final String SCHEMA = "\"schema\":";

	public static NullableSchema parseDataSchemaJSON(String dataSchema) {
		dataSchema = dataSchema.replaceAll("\n", "");
		int index = dataSchema.indexOf(SCHEMA);
		if (index < 0) {
			return null;
		}
		int startIndex = index + SCHEMA.length();
		int endIndex = dataSchema.lastIndexOf('}');
		dataSchema = dataSchema.substring(startIndex + 1, endIndex).trim();
		return gson.fromJson(dataSchema, NullableSchema.class);
	}

	public static String convertToJSON(final Object obj) {
		return gson.toJson(obj);
	}
	
	public static CDAPPipelineInfo parseWranglerPlugin(final String wranglerPluginConfig) {
		return gson.fromJson(wranglerPluginConfig, CDAPPipelineInfo.class);
	}
	
	public static void main(String args[]) {
		String pluginConfig = "{\"artifact\":{\"name\":\"cdap-data-pipeline\",\"version\":\"5.0.0\",\"scope\":\"SYSTEM\"},\"config\":{\"stages\":[{\"name\":\"Wrangler\",\"plugin\":{\"name\":\"Wrangler\",\"label\":\"Wrangler\",\"type\":\"transform\",\"artifact\":{\"name\":\"wrangler-transform\",\"version\":\"3.1.0\",\"scope\":\"SYSTEM\"},\"properties\":{\"workspaceId\":\"7012e054000b06d5d83b4366f6bd98ca\",\"directives\":\"parse-as-csv :body ',' false\\ndrop body\",\"schema\":\"{\\\"name\\\":\\\"avroSchema\\\",\\\"type\\\":\\\"record\\\",\\\"fields\\\":[{\\\"name\\\":\\\"body_1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"body_2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"body_3\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"body_4\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]}\",\"field\":\"body\",\"precondition\":\"false\",\"threshold\":\"1\"}}},{\"name\":\"File\",\"plugin\":{\"name\":\"File\",\"label\":\"File\",\"type\":\"batchsource\",\"artifact\":{\"name\":\"core-plugins\",\"version\":\"[1.7.0, 3.0.0)\",\"scope\":\"SYSTEM\"},\"properties\":{\"path\":\"file:/Users/bhupesh.goel/Documents/AnalyticsEngine/AutoFeatureEngineeringPOC/customers.csv\",\"ignoreNonExistingFolders\":\"false\",\"recursive\":\"false\",\"referenceName\":\"customers.csv\"}}}],\"connections\":[{\"from\":\"File\",\"to\":\"Wrangler\"}],\"resources\":{\"memoryMB\":1024,\"virtualCores\":1},\"driverResources\":{\"memoryMB\":1024,\"virtualCores\":1}}}";
		CDAPPipelineInfo pipeline = parseWranglerPlugin(pluginConfig);
		String dataSchema = "[{\"name\": \"etlSchemaBody\",\"schema\": {\"type\": \"record\",\"name\": \"etlSchemaBody\",\"fields\": [{\"name\": \"body_1\",\"type\": [\"string\",\"null\"]},{\"name\": \"body_2\",\"type\": [\"string\",\"null\"]},{\"name\": \"body_3\",\"type\": [\"string\",\"null\"]},{\"name\": \"body_4\",\"type\": [\"string\",\"null\"]}]}}]";
		NullableSchema schema = parseDataSchemaJSON(dataSchema);
	}

	public static Object convertToObject(String data, Class<?> class1) {
		return gson.fromJson(data, class1);
	}
}
