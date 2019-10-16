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

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.runtime.spi.provisioner.Capabilities;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategies;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategy;
import io.cdap.cdap.runtime.spi.provisioner.ProgramRun;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import io.cdap.cdap.runtime.spi.ssh.SSHContext;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Provisions a cluster using Azure HDInsight.
 */
public class HDInsightProvisioner implements Provisioner {

  private static final Logger LOG = LoggerFactory.getLogger(HDInsightProvisioner.class);
  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
    "azure-hdinsight", "Azure HDInsight",
    "Azure HDInsight is a managed, full-spectrum, open-source analytics service in the cloud for " +
      "enterprises. You can use open-source frameworks such as Hadoop, Apache Spark, Apache Hive, " +
      "LLAP, Apache Kafka, Apache Storm, R, and more.");

  @Override
  public ProvisionerSpecification getSpec() {
    return SPEC;
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    HDInsightConf.fromProperties(properties);
  }

  @Override
  public Cluster createCluster(ProvisionerContext context) throws Exception {
    // Generates and set the ssh key if it does not have one.
    // Since invocation of this method can come from a retry, we don't need to keep regenerating the keys
    SSHContext sshContext = context.getSSHContext();
    SSHKeyPair sshKeyPair = sshContext.getSSHKeyPair().orElse(null);
    if (sshKeyPair == null) {
      sshKeyPair = sshContext.generate("cdap");
      sshContext.setSSHKeyPair(sshKeyPair);
    }

    HDInsightConf conf = HDInsightConf.fromProvisionerContext(context);
    HDInsightClient client = HDInsightClient.fromConf(conf);
    String clusterName = getClusterName(context.getProgramRun(), conf.getClusterNamePrefix());

    // if it already exists, it means this is a retry. We can skip actually making the request
    Optional<Cluster> cluster = client.getCluster(clusterName);
    if (cluster.isPresent()) {
      return client.getCluster(cluster.get().getName()).get();
    }

    LOG.info("Creating Azure HDInsight cluster with name " + clusterName);
    client.createCluster(clusterName);
    return new Cluster(clusterName, ClusterStatus.CREATING, Collections.emptyList(), Collections.emptyMap());
  }

  static String getClusterName(ProgramRun programRun, String prefix) {
    String cleanedAppName = programRun.getApplication().replaceAll("[^A-Za-z0-9\\-]", "").toLowerCase();
    // 59 is max length, need to subtract the prefix and 1 extra for the '-' separating app name and run id
    int maxAppLength = 59 - prefix.length() - 1 - programRun.getRun().length();
    if (cleanedAppName.length() > maxAppLength) {
      cleanedAppName = cleanedAppName.substring(0, maxAppLength);
    }
    return prefix + cleanedAppName + "-" + programRun.getRun();
  }

  @Override
  public ClusterStatus getClusterStatus(ProvisionerContext context, Cluster cluster) throws Exception {
    return getClusterDetail(context, cluster).getStatus();
  }

  @Override
  public Cluster getClusterDetail(ProvisionerContext context, Cluster cluster) throws Exception {
    HDInsightConf conf = HDInsightConf.fromProvisionerContext(context);
    HDInsightClient client = HDInsightClient.fromConf(conf);
    Optional<Cluster> existing = client.getCluster(cluster.getName());
    return existing.orElseGet(() -> new Cluster(cluster, ClusterStatus.NOT_EXISTS));
  }

  @Override
  public void deleteCluster(ProvisionerContext context, Cluster cluster) throws Exception {
    LOG.info("Deleting Azure HDInsight cluster with name " + cluster.getName());
    HDInsightConf conf = HDInsightConf.fromProvisionerContext(context);
    HDInsightClient.fromConf(conf).deleteCluster(cluster.getName());
  }

  @Override
  public PollingStrategy getPollingStrategy(ProvisionerContext context, Cluster cluster) {
    PollingStrategy strategy = PollingStrategies.fixedInterval(5, TimeUnit.SECONDS);
    switch (cluster.getStatus()) {
      case CREATING:
        return PollingStrategies.initialDelay(strategy, 60, TimeUnit.SECONDS);
      case DELETING:
        return PollingStrategies.initialDelay(strategy, 30, TimeUnit.SECONDS);
    }
    LOG.warn("Received a request to get the polling strategy for unexpected cluster status {}", cluster.getStatus());
    return strategy;
  }

  @Override
  public Capabilities getCapabilities() {
    return new Capabilities(Collections.unmodifiableSet(new HashSet<>(Arrays.asList("fileSet", "externalDataset"))));
  }
}
