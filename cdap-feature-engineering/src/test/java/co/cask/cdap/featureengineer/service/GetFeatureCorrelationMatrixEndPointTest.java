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
package co.cask.cdap.featureengineer.service;

import co.cask.cdap.common.enums.CorrelationCoefficient;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author bhupesh.goel
 *
 */
public class GetFeatureCorrelationMatrixEndPointTest {
    
    private static final String USER_AGENT = "Mozilla/5.0";
    
    private static void testURL(String coefficientType) throws Exception {
        String url = "http://bhupesh-goel.local:11015/v3/namespaces/default/apps/FeatureEngineeringApp/services/"
                + "ManualFeatureSelectionService/methods/featureengineering/features/correlation/matrix/get?"
                + "pipelineName=InputX1ErrorTestPipeline&correlationCoefficient=" + coefficientType;
        
        HttpClient client = new DefaultHttpClient();
        HttpGet request = new HttpGet(url);
        
        // add request header
        request.addHeader("User-Agent", USER_AGENT);
        HttpResponse response = client.execute(request);
        
        System.out.println("Response Code : " + response.getStatusLine().getStatusCode());
        
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        
        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        System.out.println(result.toString());
    }
    
    public static void main(String[] args) throws Exception {
        for (CorrelationCoefficient coeff : CorrelationCoefficient.values()) {
            testURL(coeff.getName());
        }
    }
}
