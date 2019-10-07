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

import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.AzureEnvironment;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.ClusterCreateParametersExtended;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.ClusterCreateProperties;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.ClusterDefinition;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.ComputeProfile;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.HardwareProfile;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.LinuxOperatingSystemProfile;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.OSType;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.OsProfile;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.Role;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.SshProfile;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.SshPublicKey;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.StorageAccount;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.StorageProfile;
import com.microsoft.azure.management.hdinsight.v2018_06_01_preview.Tier;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Configuration for Azure HDInsight
 */
class HDInsightConf {

  // Azure Account Information
  private final String subscriptionId;
  private final String tenantId;
  private final String clientId;
  private final String clientSecret;

  // Cluster setting
  // TODO: support an auto-generated default resource group
  private final String resourceGroupName;
  // TODO: support an auto-generated default storage account
  private final String storageAccountName;
  private final String storageAccountKey;
  private final String clusterPassword;
  private final String clusterNamePrefix;
  private final String location;

  // Node config
  private final int workerNodeCount;
  private final String headNodeVmSize;
  private final String workerNodeVmSize;

  private final SSHPublicKey publicKey;

  // validation REGEX
  @SuppressWarnings("checkstyle:MemberName")
  private static final String PASSWORD_PATTERN = "^(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z])(?=.*[-@#$%^&+=~!*()_]).{10,}$";
  private static final String NAME_PATTERN = "^[a-zA-z][-a-zA-Z0-9]{2,28}[-a-zA-Z0-9]$";

  private HDInsightConf(String subscriptionId, String tenantId, String clientId, String clientSecret,
                        String resourceGroupName, String storageAccountName, String storageAccountKey,
                        String clusterPassword, String clusterNamePrefix, String location, int workerNodeCount,
                        String headNodeVmSize, String workerNodeVmSize, SSHPublicKey publicKey) {
    this.subscriptionId = subscriptionId;
    this.tenantId = tenantId;
    this.clientId = clientId;
    this.clientSecret = clientSecret;

    this.resourceGroupName = resourceGroupName;
    this.storageAccountName = storageAccountName;
    this.storageAccountKey = storageAccountKey;
    this.clusterPassword = clusterPassword;
    this.clusterNamePrefix = clusterNamePrefix;
    this.location = location;

    this.workerNodeCount = workerNodeCount;
    this.headNodeVmSize = headNodeVmSize;
    this.workerNodeVmSize = workerNodeVmSize;
    this.publicKey = publicKey;
  }

  static HDInsightConf fromProvisionerContext(ProvisionerContext context) {
    Optional<SSHKeyPair> sshKeyPair = context.getSSHContext().getSSHKeyPair();
    return create(context.getProperties(), sshKeyPair.map(SSHKeyPair::getPublicKey).orElse(null));
  }

  /**
   * Create the conf from a property map while also performing validation.
   */
  static HDInsightConf fromProperties(Map<String, String> properties) {
    return create(properties, null);
  }

  private static HDInsightConf create(Map<String, String> properties, @Nullable SSHPublicKey publicKey) {
    String subscriptionId = getString(properties, "subscriptionId");
    String tenantId = getString(properties, "tenantId");
    String clientId = getString(properties, "clientId");
    String clientSecret = getString(properties, "clientSecret");

    String resourceGroupName = getString(properties, "resourceGroupName");
    String storageAccountName = getString(properties, "storageAccountName") + ".blob.core.windows.net";
    String storageAccountKey = getString(properties, "storageAccountKey");
    String clusterPassword = getString(properties, "clusterPassword");
    String clusterNamePrefix = getString(properties, "clusterNamePrefix", "cdap-");
    String location = getString(properties, "location", "East US 2");

    // The password must be at least 10 characters in length and must contain at least one digit, one uppercase and one
    // lower case letter, one non-alphanumeric character (except characters ' " ` \).
    if (!Pattern.compile(PASSWORD_PATTERN).matcher(clusterPassword).matches()) {
      throw new IllegalArgumentException();
    }

    if (!Pattern.compile(NAME_PATTERN).matcher(clusterNamePrefix).matches()) {
      throw new IllegalArgumentException();
    }

    int workerNodeCount = getInt(properties, "workerNodeCount", 2);
    String headNodeVmSize = getString(properties, "headNodeVmSize");
    String workerNodeVmSize = getString(properties, "workerNodeVmSize");

    return new HDInsightConf(subscriptionId, tenantId, clientId, clientSecret, resourceGroupName, storageAccountName,
      storageAccountKey, clusterPassword, clusterNamePrefix, location, workerNodeCount, headNodeVmSize,
      workerNodeVmSize, publicKey);
  }

  ApplicationTokenCredentials getTokenCredentials() {
    // TODO: Support the others AzureEnvironment
    return new ApplicationTokenCredentials(
      clientId,
      tenantId,
      clientSecret,
      AzureEnvironment.AZURE);
  }

  ClusterCreateParametersExtended getClusterCreateParameters() {
    HashMap<String, HashMap<String, String>> configurations = new HashMap<>();
    HashMap<String, String> gateway = new HashMap<>();
    gateway.put("restAuthCredential.enabled_credential", "True");
    gateway.put("restAuthCredential.username", "admin");
    gateway.put("restAuthCredential.password", this.clusterPassword);
    configurations.put("gateway", gateway);

    return new ClusterCreateParametersExtended()
      .withLocation(this.location)
      .withTags(Collections.EMPTY_MAP)
      .withProperties(
        new ClusterCreateProperties()
          .withClusterVersion("3.6")
          .withOsType(OSType.LINUX)
          .withClusterDefinition(new ClusterDefinition()
            .withKind("spark")
            .withComponentVersion(ImmutableMap.of("Spark", "2.2"))
            .withConfigurations(configurations)
          )
          .withTier(Tier.STANDARD)
          .withComputeProfile(new ComputeProfile()
            .withRoles(Arrays.asList(
              new Role()
                .withName("headnode")
                .withTargetInstanceCount(1)
                .withHardwareProfile(new HardwareProfile()
                  .withVmSize(this.headNodeVmSize)
                )
                .withOsProfile(new OsProfile()
                  .withLinuxOperatingSystemProfile(new LinuxOperatingSystemProfile()
                    .withUsername("cdap")
                    .withPassword(this.clusterPassword)
                    .withSshProfile(new SshProfile()
                        .withPublicKeys(Arrays.asList(
                          new SshPublicKey()
                            .withCertificateData(this.publicKey.getKey())
                        ))
                      )
                    )
                ),
              new Role()
                .withName("workernode")
                .withTargetInstanceCount(this.workerNodeCount)
                .withHardwareProfile(new HardwareProfile()
                  .withVmSize(this.workerNodeVmSize)
                )
                .withOsProfile(new OsProfile()
                  .withLinuxOperatingSystemProfile(new LinuxOperatingSystemProfile()
                    .withUsername("cdap")
                    .withPassword(this.clusterPassword)
                    .withSshProfile(new SshProfile()
                      .withPublicKeys(Arrays.asList(
                        new SshPublicKey()
                          .withCertificateData(this.publicKey.getKey())
                      ))
                    )
                  )
                )
            ))
          )
          .withStorageProfile(new StorageProfile()
            .withStorageaccounts(Arrays.asList(
              new StorageAccount()
                .withName(this.storageAccountName)
                .withKey(this.storageAccountKey)
                .withContainer("cdap")
                .withIsDefault(true)
            ))
          )
      );
  }

  private static String getString(Map<String, String> properties, String key) {
    String val = properties.get(key);
    if (val == null) {
      throw new IllegalArgumentException(String.format("Invalid config. '%s' must be specified.", key));
    }
    return val;
  }

  private static String getString(Map<String, String> properties, String key, String defaultVal) {
    String val = properties.get(key);
    return val == null ? defaultVal : val;
  }

  private static int getInt(Map<String, String> properties, String key, int defaultVal) {
    String valStr = properties.get(key);
    if (valStr == null) {
      return defaultVal;
    }

    try {
      int val = Integer.parseInt(valStr);
      if (val < 0) {
        throw new IllegalArgumentException(
          String.format("Invalid config '%s' = '%s'. Must be a positive integer.", key, valStr));
      }
      return val;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Invalid config '%s' = '%s'. Must be a valid, positive integer.", key, valStr));
    }
  }

  String getClusterNamePrefix() {
    return clusterNamePrefix;
  }

  String getResourceGroupName() {
    return resourceGroupName;
  }

  String getSubscriptionId() {
    return subscriptionId;
  }

}
