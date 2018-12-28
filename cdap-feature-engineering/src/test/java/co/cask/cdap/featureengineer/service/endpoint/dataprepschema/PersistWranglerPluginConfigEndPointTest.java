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
package co.cask.cdap.featureengineer.service.endpoint.dataprepschema;

import co.cask.cdap.featureengineer.proto.PersistWranglerRequest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author bhupesh.goel
 *
 */
public class PersistWranglerPluginConfigEndPointTest {

    private static final String USER_AGENT = "Mozilla/5.0";
    static final Gson GSON_OBJ = new GsonBuilder().setPrettyPrinting().create();
    static final String ERROR_TABLE = "errors";
    static final String ACCOUNT_TABLE = "accounts";

    public static void main(String args[]) throws ClientProtocolException, IOException {
        String url = "http://bhupesh-goel.local:11015/v3/namespaces/default/apps/FeatureEngineeringApp/services/"
                + "DataPrepSchemaService/methods/featureengineering/accounts/wrangler/batch/config";
        System.out.println("url = " + url);
        HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost(url);

        // // add header
        post.setHeader("User-Agent", USER_AGENT);
        post.setHeader("Content-Type", "application/json");
        post.setHeader("Accept", "application/json");
        post.setHeader("X-Stream", "true");
        PersistWranglerRequest request = createAccountSchemaRequest();
        post.setEntity(new StringEntity(GSON_OBJ.toJson(request)));
        HttpResponse response = client.execute(post);
        System.out.println("Response Code : " + response.getStatusLine().getStatusCode());
        BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        String line = "";
        StringBuilder result = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            result.append(line);
        }
        System.out.println(result.toString());

        url = "http://bhupesh-goel.local:11015/v3/namespaces/default/apps/FeatureEngineeringApp/services/"
                + "DataPrepSchemaService/methods/featureengineering/errors/wrangler/batch/config";

        System.out.println("Next data schema persist request");
        client = new DefaultHttpClient();
        post = new HttpPost(url);

        // // add header
        post.setHeader("User-Agent", USER_AGENT);
        post.setHeader("Content-Type", "application/json");
        post.setHeader("Accept", "application/json");
        post.setHeader("X-Stream", "true");
        request = createErrorSchemaRequest();
        post.setEntity(new StringEntity(GSON_OBJ.toJson(request)));
        response = client.execute(post);
        System.out.println("Response Code : " + response.getStatusLine().getStatusCode());
        reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        line = "";
        result = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            result.append(line);
        }
        System.out.println(result.toString());

    }

    private static PersistWranglerRequest createAccountSchemaRequest() {
        String schema = "[\n" + "    {\n" + "        \"name\": \"etlSchemaBody\",\n" + "        \"schema\": {\n"
                + "            \"type\": \"record\",\n" + "            \"name\": \"etlSchemaBody\",\n"
                + "            \"fields\": [\n" + "                {\n"
                + "                    \"name\": \"account_id\",\n" + "                    \"type\": [\n"
                + "                        \"long\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                }\n" + "            ]\n" + "        }\n" + "    }\n"
                + "]";
        String pluginConfig = "{\"artifact\":{\"name\":\"cdap-data-pipeline\",\"version\":\"5.0.0\",\"scope\":"
                + "\"SYSTEM\""
                + "},\"config\":{\"stages\":[{\"name\":\"Wrangler\",\"plugin\":{\"name\":\"Wrangler\",\"label\":"
                + "\"Wrangler\",\"type\":\"transform\",\"artifact\":{\"name\":\"wrangler-transform\",\"version\":"
                + "\"3.1.0\",\"scope\":\"SYSTEM\"},\"properties\":{\"workspaceId\":\"5fbc6039793a92712bcb9d38a70e6013"
                + "\",\"directives\":\"rename body account_id\\nset-type :account_id long\",\"schema\":\"{"
                + "\\\"name\\\":\\\"avroSchema\\\",\\\"type\\\":\\\"record\\\",\\\"fields\\\":[{\\\"name\\\":"
                + "\\\"account_id\\\",\\\"type\\\":[\\\"long\\\",\\\"null\\\"]}]}\",\"field\":\"body\","
                + "\"precondition\":\"false\",\"threshold\":\"1\"}}},{\"name\":\"File\",\"plugin\":{\"name\":"
                + "\"File\",\"label\":\"File\",\"type\":\"batchsource\",\"artifact\":{\"name\":\"core-plugins\","
                + "\"version\":\"[1.7.0, 3.0.0)\",\"scope\":"
                + "\"SYSTEM\"},\"properties\":{\"path\":\"file:/Users/bhupesh.goel/Documents/AnalyticsEngine/"
                + "AutoFeatureEngineeringPOC/accounts.csv\",\"ignoreNonExistingFolders\":\"false\",\"recursive\":"
                + "\"false\",\"referenceName\":\"accounts.csv\"}}}],\"connections\":[{\"from\":\"File\",\"to\":"
                + "\"Wrangler\"}],\"resources\":{\"memoryMB\":1024,\"virtualCores\":1},\"driverResources\":"
                + "{\"memoryMB\":1024,\"virtualCores\":1}}}";
        PersistWranglerRequest req = new PersistWranglerRequest();
        req.setPluginConfig(pluginConfig);
        req.setSchema(schema);
        return req;
    }

    private static PersistWranglerRequest createErrorSchemaRequest() {
        String schema = "[\n" + "    {\n" + "        \"name\": \"etlSchemaBody\",\n" + "        \"schema\": {\n"
                + "            \"type\": \"record\",\n" + "            \"name\": \"etlSchemaBody\",\n"
                + "            \"fields\": [\n" + "                {\n"
                + "                    \"name\": \"error_id\",\n" + "                    \"type\": [\n"
                + "                        \"long\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                },\n" + "                {\n"
                + "                    \"name\": \"account_id\",\n" + "                    \"type\": [\n"
                + "                        \"long\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                },\n" + "                {\n"
                + "                    \"name\": \"device_mac_address\",\n" + "                    \"type\": [\n"
                + "                        \"string\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                },\n" + "                {\n"
                + "                    \"name\": \"event_is_startup\",\n" + "                    \"type\": [\n"
                + "                        \"boolean\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                },\n" + "                {\n"
                + "                    \"name\": \"event_is_visible\",\n" + "                    \"type\": [\n"
                + "                        \"boolean\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                },\n" + "                {\n"
                + "                    \"name\": \"device_video_firmware\",\n" + "                    \"type\": [\n"
                + "                        \"string\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                },\n" + "                {\n"
                + "                    \"name\": \"event_cause_category\",\n" + "                    \"type\": [\n"
                + "                        \"string\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                },\n" + "                {\n"
                + "                    \"name\": \"event_cause_id\",\n" + "                    \"type\": [\n"
                + "                        \"string\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                },\n" + "                {\n"
                + "                    \"name\": \"hour\",\n" + "                    \"type\": [\n"
                + "                        \"int\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                },\n" + "                {\n"
                + "                    \"name\": \"device_video_model\",\n" + "                    \"type\": [\n"
                + "                        \"string\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                },\n" + "                {\n"
                + "                    \"name\": \"event_hostname\",\n" + "                    \"type\": [\n"
                + "                        \"string\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                },\n" + "                {\n"
                + "                    \"name\": \"event_date_time\",\n" + "                    \"type\": [\n"
                + "                        \"string\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                },\n" + "                {\n"
                + "                    \"name\": \"ets_timestamp\",\n" + "                    \"type\": [\n"
                + "                        \"string\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                },\n" + "                {\n"
                + "                    \"name\": \"date_hour\",\n" + "                    \"type\": [\n"
                + "                        \"string\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                },\n" + "                {\n"
                + "                    \"name\": \"count\",\n" + "                    \"type\": [\n"
                + "                        \"int\",\n" + "                        \"null\"\n"
                + "                    ]\n" + "                }\n" + "            ]\n" + "        }\n" + "    }\n"
                + "]";
        String pluginConfig = "{\"artifact\":{\"name\":\"cdap-data-pipeline\",\"version\":\"5.0.0\",\"scope\":"
                + "\"SYSTEM\"},\"config\":{\"stages\":[{\"name\":\"Wrangler\",\"plugin\":{\"name\":\"Wrangler\","
                + "\"label\":\"Wrangler\",\"type\":\"transform\",\"artifact\":{\"name\":\"wrangler-transform\","
                + "\"version\":\"3.1.0\",\"scope\":\"SYSTEM\"},\"properties\":{\"workspaceId\":"
                + "\"7a3270009b45fc5ccd0fffb0c78d8624\",\"directives\":\"parse-as-csv :body ',' false\\ndrop body"
                + "\\nrename body_2 account_id\\nrename body_1 error_id\\nrename body_3 device_mac_address\\nrename "
                + "body_4 event_is_startup\\nrename body_5 event_is_visible\\nrename body_6 device_video_firmware"
                + "\\nrename body_7 event_cause_category\\nrename body_8 event_cause_id\\nrename body_9 hour\\nrename "
                + "body_10 device_video_model\\nrename body_11 event_hostname\\nrename body_12 event_date_time"
                + "\\nrename body_13 ets_timestamp\\nrename body_14 date_hour\\nrename body_15 count\\nset-type "
                + ":count integer\\nset-type :hour integer\\nset-type :error_id long\\nset-type :account_id long"
                + "\\nset-type :event_is_startup boolean\\nset-type :event_is_visible boolean\",\"schema\":\"{"
                + "\\\"name\\\":\\\"avroSchema\\\",\\\"type\\\":\\\"record\\\",\\\"fields\\\":[{\\\"name\\\":"
                + "\\\"error_id\\\",\\\"type\\\":[\\\"long\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_id\\\","
                + "\\\"type\\\":[\\\"long\\\",\\\"null\\\"]},{\\\"name\\\":\\\"device_mac_address\\\",\\\"type\\\""
                + ":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"event_is_startup\\\",\\\"type\\\":[\\\"boolean\\\""
                + ",\\\"null\\\"]},{\\\"name\\\":\\\"event_is_visible\\\",\\\"type\\\":[\\\"boolean\\\",\\\"null\\\"]}"
                + ",{\\\"name\\\":\\\"device_video_firmware\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{"
                + "\\\"name\\\":\\\"event_cause_category\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\""
                + ":\\\"event_cause_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"hour\\\","
                + "\\\"type\\\":[\\\"int\\\",\\\"null\\\"]},{\\\"name\\\":\\\"device_video_model\\\",\\\"type\\\":["
                + "\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"event_hostname\\\",\\\"type\\\":[\\\"string\\\","
                + "\\\"null\\\"]},{\\\"name\\\":\\\"event_date_time\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},"
                + "{\\\"name\\\":\\\"ets_timestamp\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":"
                + "\\\"date_hour\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"count\\\","
                + "\\\"type\\\":[\\\"int\\\",\\\"null\\\"]}]}\",\"field\":\"body\",\"precondition\":\"false\","
                + "\"threshold\":\"1\"}}},{\"name\":\"File\",\"plugin\":{\"name\":\"File\",\"label\":\"File\","
                + "\"type\":\"batchsource\",\"artifact\":{\"name\":\"core-plugins\",\"version\":\"[1.7.0, 3.0.0)\","
                + "\"scope\":\"SYSTEM\"},\"properties\":{\"path\":\"file:/Users/bhupesh.goel/Documents/AnalyticsEngine"
                + "/AutoFeatureEngineeringPOC/errors.csv\",\"ignoreNonExistingFolders\":\"false\",\"recursive\":"
                + "\"false\",\"referenceName\":\"errors.csv\"}}}],\"connections\":[{\"from\":\"File\",\"to\":"
                + "\"Wrangler\"}],\"resources\":{\"memoryMB\":1024,\"virtualCores\":1},\"driverResources\":"
                + "{\"memoryMB\":1024,\"virtualCores\":1}}}";
        PersistWranglerRequest req = new PersistWranglerRequest();
        req.setPluginConfig(pluginConfig);
        req.setSchema(schema);
        return req;
    }
}
