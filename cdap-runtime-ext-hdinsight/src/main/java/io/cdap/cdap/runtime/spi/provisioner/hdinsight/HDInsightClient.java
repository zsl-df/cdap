/*
 * Copyright © 2019 Guavus
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

package io.cdap.cdap.runtime.spi.provisioner.hdinsight;

import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.ConnectivityEndpoint;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.HDInsightClusterProvisioningState;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.implementation.ClusterInner;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.implementation.HDInsightManagementClientImpl;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.implementation.HDInsightManager;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.exceptions.OnErrorNotImplementedException;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Azure Management SDK Client
 */
public class HDInsightClient {
  private static final Logger LOG = LoggerFactory.getLogger(HDInsightClient.class);

  private final HDInsightConf conf;
  private final HDInsightManagementClientImpl client;

  static HDInsightClient fromConf(HDInsightConf conf) {
    HDInsightManagementClientImpl client = new HDInsightManagementClientImpl(conf.getTokenCredentials())
      .withSubscriptionId(conf.getSubscriptionId());
    return new HDInsightClient(conf, client);
  }

  private HDInsightClient(HDInsightConf conf, HDInsightManagementClientImpl client) {
    this.conf = conf;
    this.client = client;
  }

  void createCluster(String name) {
    try {
      client.clusters().createAsync(conf.getResourceGroupName(), name, conf.getClusterCreateParameters(name))
        .subscribeOn(Schedulers.newThread())
        .subscribe();
    } catch (OnErrorNotImplementedException e) {
      LOG.error(e.getMessage());
    }
  }

  Optional<Cluster> getCluster(String name) {
    Optional<ClusterInner> clusterInner = getClusterInner(name);
    if (!clusterInner.isPresent()) {
      return Optional.empty();
    }

    Optional<List<ConnectivityEndpoint>> endpoints = Optional.ofNullable(clusterInner.get().properties()
      .connectivityEndpoints());
    List<Node> nodes = new ArrayList<>();
    if (endpoints.isPresent()) {
      for (ConnectivityEndpoint endpoint : endpoints.get()) {
        if (endpoint.name().equals("SSH")) {
          nodes.add(new Node("id", Node.Type.MASTER, endpoint.location(),
            System.currentTimeMillis(), Collections.emptyMap()));
        }
      }
    }

    return Optional.of(new Cluster(name, getClusterStatus(clusterInner.get()), nodes, Collections.emptyMap()));
  }

  private ClusterStatus getClusterStatus(ClusterInner clusterInner) {
    switch (clusterInner.properties().provisioningState()) {
      case IN_PROGRESS:
        return ClusterStatus.CREATING;
      case FAILED:
        return ClusterStatus.FAILED;
      case DELETING:
        return ClusterStatus.DELETING;
    }

    switch (clusterInner.properties().clusterState()) {
      case "Running":
        return ClusterStatus.RUNNING;
      case "Deleting":
      case "DeleteQueued":
        return ClusterStatus.DELETING;
      default:
        return ClusterStatus.ORPHANED;
    }
  }

  Optional<ClusterInner> getClusterInner(String name) {
    ClusterInner cluster = client.clusters().getByResourceGroup(conf.getResourceGroupName(), name);
    Optional<ClusterInner> clusterInner = Optional.ofNullable(cluster);
    return clusterInner;
  }

  void deleteCluster(String name) {
    Optional<ClusterInner> cluster;
    cluster = Optional.ofNullable(client.clusters().getByResourceGroup(conf.getResourceGroupName(), name));
    if (cluster.isPresent()) {
      client.clusters().deleteAsync(conf.getResourceGroupName(), name)
        .subscribeOn(Schedulers.newThread())
        .subscribe();
    }
  }
}
