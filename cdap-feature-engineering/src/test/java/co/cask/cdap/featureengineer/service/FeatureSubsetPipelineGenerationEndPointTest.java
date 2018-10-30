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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import co.cask.cdap.featureengineer.proto.FeatureSelectionRequest;

/**
 * @author bhupesh.goel
 *
 */
public class FeatureSubsetPipelineGenerationEndPointTest {
	private static final String USER_AGENT = "Mozilla/5.0";
	static final Gson gsonObj = new GsonBuilder().setPrettyPrinting().create();

	public static void main(String[] args) throws IOException {
		String featureFile = "/tmp/generatedRandomFeatureSet";
		File f = new File(featureFile);
		BufferedReader b = new BufferedReader(new FileReader(f));
		String readLine = "";
		List<String> features = new LinkedList<String>();
		while ((readLine = b.readLine()) != null) {
			features.add(readLine.trim());
		}
		FeatureSelectionRequest request = new FeatureSelectionRequest();
		request.setSelectedFeatures(features);
		request.setFeatureSelectionPipeline("InputX1ErrorSelectionTestPipeline");
		request.setFeatureEngineeringPipeline("InputX1ErrorTestPipeline");

		String url = "http://bhupesh-goel.local:11015/v3/namespaces/default/apps/FeatureEngineeringApp/services/AutoFeatureEngineeringService/methods/featureengineering/InputX1ErrorTestPipeline/features/selected/create/pipeline";
		System.out.println("url = " + url);
		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost(url);

		// // add header
		post.setHeader("User-Agent", USER_AGENT);
		post.setHeader("Content-Type", "application/json");
		post.setHeader("Accept", "application/json");
		post.setHeader("X-Stream", "true");
		post.setEntity(new StringEntity(gsonObj.toJson(request)));
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

}
