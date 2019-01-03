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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author bhupesh.goel
 *
 */
public class AutoFeatureGenerationDeleteEndPointTest {
    
    private static final String USER_AGENT = "Mozilla/5.0";
    static final Gson GSON_OBJ = new GsonBuilder().setPrettyPrinting().create();
    static final String ERROR_TABLE = "errors";
    static final String ACCOUNT_TABLE = "accounts";
    
    public static void main(String args[]) throws ClientProtocolException, IOException {
        String url = "http://192.168.156.36:11015/v3/namespaces/default/apps/FeatureEngineeringApp/services/"
                + "AutoFeatureGenerationService/methods/featureengineering/InputX1ErrorTestPipeline/features/delete";
        System.out.println("url = " + url);
        HttpClient client = new DefaultHttpClient();
        HttpDelete delete = new HttpDelete(url);
        
        // // add header
        delete.setHeader("User-Agent", USER_AGENT);
        delete.setHeader("Content-Type", "application/json");
        delete.setHeader("Accept", "application/json");
        delete.setHeader("X-Stream", "true");
        HttpResponse response = client.execute(delete);
        System.out.println("Response Code : " + response.getStatusLine().getStatusCode());
        BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        String line = "";
        StringBuilder result = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            result.append(line);
        }
        System.out.println(result.toString());
    }
}
