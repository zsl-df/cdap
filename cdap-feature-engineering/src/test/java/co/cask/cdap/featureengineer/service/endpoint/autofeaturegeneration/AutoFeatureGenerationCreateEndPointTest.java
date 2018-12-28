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
package co.cask.cdap.featureengineer.service.endpoint.autofeaturegeneration;

import co.cask.cdap.featureengineer.proto.FeatureGenerationRequest;
import co.cask.cdap.featureengineer.request.pojo.MultiFieldAggregationInput;
import co.cask.cdap.featureengineer.request.pojo.MultiSchemaColumn;
import co.cask.cdap.featureengineer.request.pojo.Relation;
import co.cask.cdap.featureengineer.request.pojo.SchemaColumn;

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
import java.util.LinkedList;
import java.util.List;

/**
 * @author bhupesh.goel
 *
 */
public class AutoFeatureGenerationCreateEndPointTest {

    private static final String USER_AGENT = "Mozilla/5.0";
    static final Gson GSON_OBJ = new GsonBuilder().setPrettyPrinting().create();
    static final String ERROR_TABLE = "errors";
    static final String ACCOUNT_TABLE = "accounts";

    public static void main(String args[]) throws ClientProtocolException, IOException {
        String url = "http://bhupesh-goel.local:11015/v3/namespaces/default/apps/FeatureEngineeringApp/services/"
                + "AutoFeatureGenerationService/methods/featureengineering/InputX1ErrorTestPipeline/features/create";
        System.out.println("url = " + url);
        HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost(url);

        // // add header
        post.setHeader("User-Agent", USER_AGENT);
        post.setHeader("Content-Type", "application/json");
        post.setHeader("Accept", "application/json");
        post.setHeader("X-Stream", "true");
        FeatureGenerationRequest request = createNewRequest();
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
    }

    private static FeatureGenerationRequest createNewRequest() {
        FeatureGenerationRequest request = new FeatureGenerationRequest();
        request.setPipelineRunName("InputX1ErrorTestPipeline");
        List<SchemaColumn> timeIndexColumns = new LinkedList<SchemaColumn>();
        timeIndexColumns.add(new SchemaColumn(ERROR_TABLE, "ets_timestamp"));
        request.setTimeIndexColumns(timeIndexColumns);
        List<SchemaColumn> createEntities = new LinkedList<SchemaColumn>();
        createEntities.add(new SchemaColumn(ACCOUNT_TABLE, "account_id"));
        request.setCreateEntities(createEntities);
        // errors.device_video_firmware,errors.event_cause_category,errors.event_cause_id,errors.device_video_model,
        // errors.hour
        List<SchemaColumn> categoricalColumns = new LinkedList<SchemaColumn>();
        categoricalColumns.add(new SchemaColumn(ERROR_TABLE, "device_video_firmware"));
        categoricalColumns.add(new SchemaColumn(ERROR_TABLE, "event_cause_category"));
        categoricalColumns.add(new SchemaColumn(ERROR_TABLE, "event_cause_id"));
        categoricalColumns.add(new SchemaColumn(ERROR_TABLE, "device_video_model"));
        categoricalColumns.add(new SchemaColumn(ERROR_TABLE, "hour"));
        request.setCategoricalColumns(categoricalColumns);
        List<String> dataSchemaNames = new LinkedList<String>();
        dataSchemaNames.add("accounts");
        // dataSchemaNames.add("5e1f4773cbd4174d0b07169ad7ab9916");
        dataSchemaNames.add("errors");
        request.setDataSchemaNames(dataSchemaNames);
        request.setDfsDepth(4);
        List<SchemaColumn> ignoreColumns = new LinkedList<SchemaColumn>();
        ignoreColumns.add(new SchemaColumn(ERROR_TABLE, "ets_timestamp"));
        request.setIgnoreColumns(ignoreColumns);
        // accounts.account_id,errors.error_id
        List<SchemaColumn> indexes = new LinkedList<SchemaColumn>();
        indexes.add(new SchemaColumn(ACCOUNT_TABLE, "account_id"));
        indexes.add(new SchemaColumn(ERROR_TABLE, "error_id"));
        request.setIndexes(indexes);
        // errors.event_date_time;errors.date_hour,errors.ets_timestamp;errors.date_hour
        List<MultiSchemaColumn> multiFieldTransformationFunctionInputs = new LinkedList<>();
        List<SchemaColumn> temp = new LinkedList<>();
        temp.add(new SchemaColumn(ERROR_TABLE, "event_date_time"));
        temp.add(new SchemaColumn(ERROR_TABLE, "date_hour"));
        MultiSchemaColumn multiSchemaColumn = new MultiSchemaColumn();
        multiSchemaColumn.setColumns(temp);
        multiFieldTransformationFunctionInputs.add(multiSchemaColumn);
        multiSchemaColumn = new MultiSchemaColumn();
        temp = new LinkedList<>();
        temp.add(new SchemaColumn(ERROR_TABLE, "ets_timestamp"));
        temp.add(new SchemaColumn(ERROR_TABLE, "date_hour"));
        multiSchemaColumn.setColumns(temp);
        multiFieldTransformationFunctionInputs.add(multiSchemaColumn);
        request.setMultiFieldTransformationFunctionInputs(multiFieldTransformationFunctionInputs);
        // accounts.account_id:errors.account_id:errors.event_cause_id;errors.event_cause_category,accounts.account_id
        // :errors.account_id:errors.device_video_firmware;errors.device_video_model
        List<MultiFieldAggregationInput> multiFieldAggregationFunctionInputs = new LinkedList<>();
        MultiFieldAggregationInput multiFieldAggregationInput = new MultiFieldAggregationInput();
        temp = new LinkedList<>();
        temp.add(new SchemaColumn(ERROR_TABLE, "event_cause_id"));
        temp.add(new SchemaColumn(ERROR_TABLE, "event_cause_category"));
        multiFieldAggregationInput.setSourceColumns(temp);
        multiFieldAggregationInput.setDestinationColumn(new SchemaColumn(ACCOUNT_TABLE, "account_id"));
        multiFieldAggregationInput.setGroupByColumn(new SchemaColumn(ERROR_TABLE, "account_id"));
        multiFieldAggregationFunctionInputs.add(multiFieldAggregationInput);
        multiFieldAggregationInput = new MultiFieldAggregationInput();
        temp = new LinkedList<>();
        temp.add(new SchemaColumn(ERROR_TABLE, "device_video_firmware"));
        temp.add(new SchemaColumn(ERROR_TABLE, "device_video_model"));
        multiFieldAggregationInput.setSourceColumns(temp);
        multiFieldAggregationInput.setDestinationColumn(new SchemaColumn(ACCOUNT_TABLE, "account_id"));
        multiFieldAggregationInput.setGroupByColumn(new SchemaColumn(ERROR_TABLE, "account_id"));
        multiFieldAggregationFunctionInputs.add(multiFieldAggregationInput);
        request.setMultiFieldAggregationFunctionInputs(multiFieldAggregationFunctionInputs);
        // accounts.account_id:errors.account_id
        List<Relation> relationShips = new LinkedList<>();
        Relation rel = new Relation();
        rel.setColumn1(new SchemaColumn(ACCOUNT_TABLE, "account_id"));
        rel.setColumn2(new SchemaColumn(ERROR_TABLE, "account_id"));
        relationShips.add(rel);
        request.setRelationShips(relationShips);

        request.setTargetEntity(ACCOUNT_TABLE);
        request.setTargetEntityFieldId("account_id");
        // errors.event_date_time,errors.ets_timestamp,errors.date_hour
        List<SchemaColumn> timestampColumns = new LinkedList<SchemaColumn>();
        timestampColumns.add(new SchemaColumn(ERROR_TABLE, "event_date_time"));
        timestampColumns.add(new SchemaColumn(ERROR_TABLE, "ets_timestamp"));
        timestampColumns.add(new SchemaColumn(ERROR_TABLE, "date_hour"));
        request.setTimestampColumns(timestampColumns);
        List<Integer> trainingWindows = new LinkedList<Integer>();
        trainingWindows.add(1);
        trainingWindows.add(24);
        trainingWindows.add(168);
        request.setTrainingWindows(trainingWindows);
        request.setWindowEndTime("2018-02-28 23:50:00");
        return request;
    }
}
