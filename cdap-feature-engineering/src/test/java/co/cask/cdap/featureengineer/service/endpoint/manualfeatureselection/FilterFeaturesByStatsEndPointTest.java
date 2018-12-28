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

import co.cask.cdap.common.enums.FeatureSTATS;
import co.cask.cdap.featureengineer.proto.FilterFeaturesByStatsRequest;
import co.cask.cdap.featureengineer.request.pojo.CompositeType;
import co.cask.cdap.featureengineer.request.pojo.StatsFilter;
import co.cask.cdap.featureengineer.request.pojo.StatsFilterType;

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
public class FilterFeaturesByStatsEndPointTest {

    private static final String USER_AGENT = "Mozilla/5.0";
    private static final Gson GSON_OBJ = new GsonBuilder().setPrettyPrinting().create();
    private static final String ERROR_TABLE = "errors";
    private static final String ACCOUNT_TABLE = "accounts";

    private static Integer getIntValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Double) {
            return ((Double) value).intValue();
        } else if (value instanceof Long) {
            return ((Long) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        return (int) value;
    }

    public static void main(String args[]) throws ClientProtocolException, IOException {
        String url = "http://bhupesh-goel.local:11015/v3/namespaces/default/apps/FeatureEngineeringApp/services"
                + "/ManualFeatureSelectionService/methods/featureengineering/InputX1ErrorTestPipeline/"
                + "features/filter";
        System.out.println("url = " + url);
        HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost(url);

        // // add header
        post.setHeader("User-Agent", USER_AGENT);
        post.setHeader("Content-Type", "application/json");
        post.setHeader("Accept", "application/json");
        post.setHeader("X-Stream", "true");
        FilterFeaturesByStatsRequest request = createNewRequest(args[0]);

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

    private static FilterFeaturesByStatsRequest createNewRequest(String args) {
        FilterFeaturesByStatsRequest request = new FilterFeaturesByStatsRequest();

        if (args.equals("0") || args.equals("2")) {
            request.setCompositeType(CompositeType.AND);
        } else if (args.equals("1")) {
            request.setCompositeType(CompositeType.OR);
        }

        request.setOrderByStat(FeatureSTATS.InterQuartilePercentile);
        List<StatsFilter> filterList = new LinkedList<>();
        request.setFilterList(filterList);
        if (!args.equals("3")) {
            request.setComposite(true);
            StatsFilter singleLimit = new StatsFilter();
            singleLimit.setFilterType(StatsFilterType.TopN);
            singleLimit.setLowerLimit(0);
            singleLimit.setUpperLimit(10);
            singleLimit.setStatsName(FeatureSTATS.Variance);
            filterList.add(singleLimit);
            if (!args.equals("2")) {
                singleLimit = new StatsFilter();
                singleLimit.setFilterType(StatsFilterType.LowN);
                singleLimit.setLowerLimit(0);
                singleLimit.setUpperLimit(10);
                singleLimit.setStatsName(FeatureSTATS.NumOfNonZeros);
                filterList.add(singleLimit);

                singleLimit = new StatsFilter();
                singleLimit.setFilterType(StatsFilterType.LowN);
                singleLimit.setLowerLimit(0);
                singleLimit.setUpperLimit(10);
                singleLimit.setStatsName(FeatureSTATS.MostFrequentEntry);
                filterList.add(singleLimit);
            }
            if (args.equals("2")) {
                singleLimit = new StatsFilter();
                singleLimit.setFilterType(StatsFilterType.TopN);
                singleLimit.setLowerLimit(0);
                singleLimit.setUpperLimit(10);
                singleLimit.setStatsName(FeatureSTATS.Variance);
                filterList.add(singleLimit);
            }
            StatsFilter rangeLimit = new StatsFilter();
            rangeLimit.setFilterType(StatsFilterType.Range);
            rangeLimit.setLowerLimit(0.0);
            rangeLimit.setUpperLimit(1);
            rangeLimit.setStatsName(FeatureSTATS.InterQuartilePercentile);
            filterList.add(rangeLimit);

            rangeLimit = new StatsFilter();
            rangeLimit.setFilterType(StatsFilterType.Range);
            rangeLimit.setLowerLimit(0);
            rangeLimit.setUpperLimit(50);
            rangeLimit.setStatsName(FeatureSTATS.LeastFrequentWordCount);
            filterList.add(rangeLimit);
        }

        return request;
    }

    private static Long getLongValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Double) {
            return ((Double) value).longValue();
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof String) {
            return Long.parseLong(value.toString());
        }
        return (Long) value;
    }
}
