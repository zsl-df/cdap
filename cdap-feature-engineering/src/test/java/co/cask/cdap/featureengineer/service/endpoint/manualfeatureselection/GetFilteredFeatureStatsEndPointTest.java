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
package co.cask.cdap.featureengineer.service.endpoint.manualfeatureselection;

import co.cask.cdap.featureengineer.proto.FilterFeaturesByStatsRequest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author bhupesh.goel
 *
 */
public class GetFilteredFeatureStatsEndPointTest {
    private static final Gson GSON_OBJ = new GsonBuilder().create();
    private static final String USER_AGENT = "Mozilla/5.0";
    
    public static void main(String[] args) throws ClientProtocolException, IOException {
        String url = "http://bhupesh-goel.local:11015/v3/namespaces/default/apps/FeatureEngineeringApp/services/"
                + "ManualFeatureSelectionService/methods/featureengineering/InputX1ErrorTestPipeline/features/stats/"
                + "filtered/get";
        
        HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost(url);
        
        // // add header
        post.setHeader("User-Agent", USER_AGENT);
        post.setHeader("Content-Type", "application/json");
        post.setHeader("Accept", "application/json");
        post.setHeader("X-Stream", "true");
        
        List<String> request = createNewRequest();
        
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

    private static List<String> createNewRequest() {
        List<String> features = new ArrayList<String>();
        features.add("plusonelog_valuecount_errors_event_cause_id__x2_empty_purchases_message__168");
        features.add("valuecount_errors_hour__16_168");
        features.add("sum_errors_numwords_device_mac_address___1");
        features.add("catcrossproduct_event_cause_id_event_cause_category_xre-07002__xre_24___dummyCoding___abc");
        return features;
    }
}
