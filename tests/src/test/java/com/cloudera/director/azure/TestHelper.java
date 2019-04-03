/*
 * Copyright (c) 2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.director.azure;

import static com.cloudera.director.azure.Configurations.AZURE_DEFAULT_USER_AGENT_GUID;

import com.cloudera.director.azure.compute.credentials.AzureCloudEnvironment;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty;
import com.cloudera.director.azure.compute.provider.AzureComputeProviderConfigurationProperty;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.Azure;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.AvailabilitySetSkuTypes;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.msi.implementation.MSIManager;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.SecurityRuleProtocol;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.resources.fluentcore.arm.Region;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v2.compute.ComputeInstanceTemplate;
import com.cloudera.director.spi.v2.model.InstanceTemplate;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v2.provider.Launcher;
import com.google.common.io.BaseEncoding;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience functions for grabbing system properties / configuration for test.
 */
public class TestHelper {

  private static final Logger LOG = LoggerFactory.getLogger(TestHelper.class);

  // CLI parameter names
  private static final String LIVE_TEST_RESOURCE_GROUP_KEY = "azure.live.rg";
  private static final String LIVE_TEST_REGION_KEY = "azure.live.region";
  private static final String LIVE_TEST_SSH_PUBLIC_KEY_PATH = "test.azure.sshPublicKeyPath";
  private static final String LIVE_TEST_SSH_PRIVATE_KEY_PATH = "test.azure.sshPrivateKeyPath";

  // Default fields for all tests
  public static final String TEST_REGION = System.getProperty(LIVE_TEST_REGION_KEY) != null ?
      System.getProperty(LIVE_TEST_REGION_KEY) : "eastus";
  public static final String TEST_RESOURCE_GROUP =
      System.getProperty(LIVE_TEST_RESOURCE_GROUP_KEY);
  public static final String LIVE_TEST_SUBSCRIPTION_ID =
      System.getProperty(AzureCredentialsConfiguration.SUBSCRIPTION_ID.unwrap().getConfigKey());

  public static final String TEST_EMPTY_CONFIG = "";
  public static final String TEST_CENTOS_IMAGE_URN = "cloudera:cloudera-centos-os:7_4:latest";
  public static final String TEST_RHEL_IMAGE_URN = "RedHat:RHEL:7.4:latest";
  public static final String TEST_CENTOS_IMAGE_NAME = "cloudera-centos-74-latest";
  public static final String TEST_VM_SIZE = "Standard_D8S_v3";
  public static final String TEST_DATA_DISK_SIZE = "512";
  public static final String TEST_AVAILABILITY_SET_MANAGED = "managedAS";
  public static final String TEST_AVAILABILITY_SET_UNMANAGED = "unmanagedAS";
  public static final String TEST_NETWORK_SECURITY_GROUP = "nsg";
  public static final String TEST_VIRTUAL_NETWORK = "vn";
  public static final String TEST_SUBNET = "default";
  public static final String TEST_HOST_FQDN_SUFFIX = "cdh-cluster.internal";
  public static final String TEST_USER_ASSIGNED_MSI_NAME = "ua-msi";

  public static final String TEST_CUSTOM_DATA_UNENCODED = "#!/bin/sh\ntouch /tmp/jason\nexit 0";
  public static final String TEST_CUSTOM_DATA_ENCODED =
      BaseEncoding.base64().encode(TEST_CUSTOM_DATA_UNENCODED.getBytes(StandardCharsets.UTF_8));

  // Fields used for tests
  static final String TEST_SSH_PUBLIC_KEY = getSshKey(LIVE_TEST_SSH_PUBLIC_KEY_PATH);
  static final String TEST_SSH_PRIVATE_KEY = getSshKey(LIVE_TEST_SSH_PRIVATE_KEY_PATH);

  // State variables
  private static boolean existingRg = false;
  private static boolean rgDeleteFailed = false;

  /**
   * @return true if run live test flag is set.
   */
  public static boolean runLiveTests() {
    String liveString = System.getProperty("test.azure.live");
    return Boolean.parseBoolean(liveString);
  }

  /**
   * @return true to run orphaned resource cleanup tests.
   */
  public static boolean runOrphanedResourceCleanup() {
    String liveString = System.getProperty("test.azure.orphanedResourceCleanup");
    return Boolean.parseBoolean(liveString);
  }

  /**
   * @return tag name of the VM to be cleaned up
   */
  public static String resourceCleanupVmTagName() {
    return System.getProperty("test.azure.resourceCleanupVmTagName");
  }

  /**
   * @return tag value of the VM to be cleaned up
   */
  public static String resourceCleanupVmTagValue() {
    return System.getProperty("test.azure.resourceCleanupVmTagValue");
  }

  /**
   * Helper method to reset azurePluginConfig using reflection.
   *
   * @throws Exception if setting via reflection does not work
   */
  public static void setAzurePluginConfigNull() throws Exception {
    Field field = AzurePluginConfigHelper.class.getDeclaredField("azurePluginConfig");
    field.setAccessible(true);
    field.set(null, null);
  }

  /**
   * Helper method to reset configurableImages using reflection.
   *
   * @throws Exception if setting via reflection does not work
   */
  public static void setConfigurableImagesNull() throws Exception {
    Field field = AzurePluginConfigHelper.class.getDeclaredField("configurableImages");
    field.setAccessible(true);
    field.set(null, null);
  }

  private static String getSshKey(String key) {
    if (runLiveTests()) {
      try {
        return new String(
            Files.readAllBytes(Paths.get(System.getProperty(key))), StandardCharsets.UTF_8);
      } catch (Exception e) {
        LOG.error("Error with required live test argument '" + key + "':", e);
        throw new RuntimeException("Error with required live test argument '" + key + "':", e);
      }
    }

    // the ssh keys are only used for live tests, don't worry about it being null for UTs
    return null;
  }

  /**
   * Create an AzureCredentials object using stored credentials.
   *
   * @return an AzureCredentials object
   */
  public static AzureCredentials getAzureCredentials() {
    Map<String, String> map = buildBaseCredentialsMap();

    Launcher launcher = new AzureLauncher();
    return new AzureCredentials(new SimpleConfiguration(map),
        launcher.getLocalizationContext(Locale.getDefault()));
  }

  /**
   * Build the map of credentials required for authentication.
   *
   * @return the map of credential fields
   */
  private static Map<String, String> buildBaseCredentialsMap() {
    Map<String, String> map = new HashMap<>();
    // use the default cloud
    map.put(AzureCredentialsConfiguration.AZURE_CLOUD_ENVIRONMENT.unwrap().getConfigKey(),
        AzureCloudEnvironment.AZURE);
    // pull other fields from command line parameters
    map.put(AzureCredentialsConfiguration.SUBSCRIPTION_ID.unwrap().getConfigKey(),
        System.getProperty(AzureCredentialsConfiguration.SUBSCRIPTION_ID.unwrap().getConfigKey()));
    map.put(AzureCredentialsConfiguration.CLIENT_ID.unwrap().getConfigKey(),
        System.getProperty(AzureCredentialsConfiguration.CLIENT_ID.unwrap().getConfigKey()));
    map.put(AzureCredentialsConfiguration.TENANT_ID.unwrap().getConfigKey(),
        System.getProperty(AzureCredentialsConfiguration.TENANT_ID.unwrap().getConfigKey()));
    map.put(AzureCredentialsConfiguration.CLIENT_SECRET.unwrap().getConfigKey(),
        System.getProperty(AzureCredentialsConfiguration.CLIENT_SECRET.unwrap().getConfigKey()));
    // always use the default user-agent
    map.put(AzureCredentialsConfiguration.USER_AGENT.unwrap().getConfigKey(),
        AZURE_DEFAULT_USER_AGENT_GUID);

    return map;
  }

  public static Map<String, String> buildTagMap() {
    Map<String, String> tags = new HashMap<>();
    tags.put("username", System.getProperty("user.name"));
    tags.put("owner", "Cloudera Altus Director Azure Plugin Live Test");

    return tags;
  }

  /**
   * Helper method to build a Director Configuration in Map form with pre-populated fields for unit
   * tests. This Map can be used to modify config for specific tests before turning it into a
   * SimpleConfiguration.
   *
   * @return a Director config as a Map used for unit tests
   */
  public static Map<String, String> buildValidDirectorUnitTestMap() {
    Map<String, String> map = new HashMap<>();
    map.put(InstanceTemplate.InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX
        .unwrap().getConfigKey(), AzureCloudProvider.ID);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        TEST_CENTOS_IMAGE_URN);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.VMSIZE.unwrap().getConfigKey(),
        TEST_VM_SIZE);
    map.put(ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_USERNAME
        .unwrap().getConfigKey(), "cloudera");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP.unwrap()
        .getConfigKey(), "computeRG");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP
        .unwrap().getConfigKey(), "vnRG");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK.unwrap()
        .getConfigKey(), TEST_VIRTUAL_NETWORK);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME.unwrap().getConfigKey(),
        "subnet");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.HOST_FQDN_SUFFIX.unwrap()
        .getConfigKey(), TEST_HOST_FQDN_SUFFIX);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP
        .unwrap().getConfigKey(), "nsgRG");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP.unwrap()
        .getConfigKey(), TEST_NETWORK_SECURITY_GROUP);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP.unwrap().getConfigKey(),
        "No");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap()
        .getConfigKey(), TEST_AVAILABILITY_SET_MANAGED);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(),
        "Yes");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE.unwrap()
        .getConfigKey(), Configurations.AZURE_DEFAULT_STORAGE_TYPE);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT.unwrap()
        .getConfigKey(), "2");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_SIZE.unwrap()
        .getConfigKey(), String.valueOf(Configurations.AZURE_DEFAULT_DATA_DISK_SIZE));
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_NAME.unwrap().getConfigKey(),
        TEST_EMPTY_CONFIG);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_RESOURCE_GROUP.unwrap().getConfigKey(),
        TEST_EMPTY_CONFIG);

    return map;
  }

  /**
   * Helper method to build a Director Configuration with pre-populated fields for unit tests.
   *
   * @return Director SimpleConfiguration used for unit tests
   */
  public static SimpleConfiguration buildValidDirectorUnitTestConfig() {
    return new SimpleConfiguration(buildValidDirectorUnitTestMap());
  }

  /**
   * Helper method to build a Director Configuration in Map form with pre-populated fields for live
   * tests. This Map can be used to modify config for specific tests before turning it into a
   * SimpleConfiguration.
   *
   * @return Director config as a Map used for live tests
   */
  public static Map<String, String> buildValidDirectorLiveTestMap() {
    Map<String, String> map = buildBaseCredentialsMap();

    // The default resources assumed to be in the Resource Group used for live testing
    map.put(InstanceTemplate.InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX.unwrap().getConfigKey(),
        "director");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        TEST_CENTOS_IMAGE_URN);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.VMSIZE.unwrap().getConfigKey(),
        TEST_VM_SIZE);
    map.put(ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken.SSH_USERNAME
        .unwrap().getConfigKey(), "cloudera");
    map.put(ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken
        .SSH_OPENSSH_PUBLIC_KEY.unwrap().getConfigKey(), TEST_SSH_PUBLIC_KEY);
    map.put(AzureComputeProviderConfigurationProperty.REGION.unwrap().getConfigKey(),
        TEST_REGION);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP.unwrap()
        .getConfigKey(), TEST_RESOURCE_GROUP);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK_RESOURCE_GROUP
        .unwrap().getConfigKey(), TEST_RESOURCE_GROUP);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.VIRTUAL_NETWORK.unwrap()
        .getConfigKey(), TEST_VIRTUAL_NETWORK);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.SUBNET_NAME.unwrap().getConfigKey(),
        TEST_SUBNET);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.HOST_FQDN_SUFFIX.unwrap()
        .getConfigKey(), "cdh-cluster.internal");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP_RESOURCE_GROUP
            .unwrap().getConfigKey(), TEST_RESOURCE_GROUP);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.NETWORK_SECURITY_GROUP.unwrap()
        .getConfigKey(), TEST_NETWORK_SECURITY_GROUP);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP.unwrap().getConfigKey(),
        "Yes");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.AVAILABILITY_SET.unwrap()
        .getConfigKey(), TEST_AVAILABILITY_SET_MANAGED);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap().getConfigKey(),
        "Yes");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE.unwrap()
        .getConfigKey(), Configurations.AZURE_DEFAULT_STORAGE_TYPE);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT.unwrap()
        .getConfigKey(), "2");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_SIZE.unwrap()
        .getConfigKey(), TEST_DATA_DISK_SIZE);

    // hidden config, set to emptystring by default so tests are more complete
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_NAME.unwrap().getConfigKey(),
        TEST_EMPTY_CONFIG);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USER_ASSIGNED_MSI_RESOURCE_GROUP.unwrap().getConfigKey(),
        TEST_EMPTY_CONFIG);
    map.put(AzureComputeInstanceTemplateConfigurationProperty.WITH_STATIC_PRIVATE_IP_ADDRESS.unwrap().getConfigKey(),
        "Yes");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.WITH_ACCELERATED_NETWORKING.unwrap().getConfigKey(),
        "No");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_DATA_ENCODED.unwrap().getConfigKey(), "");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_DATA_UNENCODED.unwrap().getConfigKey(), "");

    return map;
  }

  /**
   * Helper method to build a Director Configuration with pre-populated fields for live tests.
   *
   * @return Director SimpleConfiguration used for live tests
   */
  public static SimpleConfiguration buildValidDirectorLiveTestConfig() {
    return new SimpleConfiguration(buildValidDirectorLiveTestMap());
  }

  /**
   * Builds a set of resources to sandbox live tests. Resources are only created if the Resource
   * Group does not already exist:
   * - a Resource Group
   * - a managed Availability Set (configured for Managed Disks)
   * - an unmanaged Availability Set (configured for Storage Accounts)
   * - a Network Security Group
   * - a User Assigned MSI
   *
   * @param credentials to get the Azure and MSIManager objects used to interact with Azure
   */
  public static void buildLiveTestEnvironment(AzureCredentials credentials) {
    LOG.info("buildLiveTestEnvironment");
    Azure azure = credentials.authenticate();

    Region region = Region.findByLabelOrName(TEST_REGION) != null ?
        Region.findByLabelOrName(TEST_REGION) : Region.US_EAST;

    // short-circuit if deleting the Resource Group has failed in the past
    if (rgDeleteFailed) {
      String errorMessage = String.format("Forcing test failure and early termination due to " +
          "Resource Group %s delete failure on earlier test. See logs", TEST_RESOURCE_GROUP);

      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage);
    }

    LOG.info("Building Test Environment in Resource Group {}.", TEST_RESOURCE_GROUP);
    try {
      // if the RG already exists then assume it's built manually and don't create or delete it
      if (azure.resourceGroups().checkExistence(TEST_RESOURCE_GROUP)) {
        LOG.info("Resource Group {} already exists, skipping resource creation.",
            TEST_RESOURCE_GROUP);

        existingRg = true;
        return;
      }

      // resource group
      LOG.info("Building Resource Group {}", TEST_RESOURCE_GROUP);
      azure.resourceGroups()
          .define(TEST_RESOURCE_GROUP)
          .withRegion(region)
          .create();

      // availability set for managed disks
      LOG.info("Building managed Availability Set {}", TEST_AVAILABILITY_SET_MANAGED);
      azure.availabilitySets()
          .define(TEST_AVAILABILITY_SET_MANAGED)
          .withRegion(region)
          .withExistingResourceGroup(TEST_RESOURCE_GROUP)
          .withSku(AvailabilitySetSkuTypes.MANAGED)
          .create();

      // availability set for storage accounts
      LOG.info("Building unmanaged Availability Set {}", TEST_AVAILABILITY_SET_UNMANAGED);
      azure.availabilitySets()
          .define(TEST_AVAILABILITY_SET_UNMANAGED)
          .withRegion(region)
          .withExistingResourceGroup(TEST_RESOURCE_GROUP)
          .withSku(AvailabilitySetSkuTypes.UNMANAGED)
          .create();

      // NSG
      LOG.info("Building Network Security Group {}", TEST_NETWORK_SECURITY_GROUP);
      azure.networkSecurityGroups()
          .define(TEST_NETWORK_SECURITY_GROUP)
          .withRegion(region)
          .withExistingResourceGroup(TEST_RESOURCE_GROUP)
          .defineRule("ssh")
          .allowInbound()
          .fromAnyAddress()
          .fromAnyPort()
          .toAnyAddress()
          .toPort(22)
          .withProtocol(SecurityRuleProtocol.TCP)
          .withPriority(100)
          .withDescription("Allow SSH")
          .attach()
          .create();

      // VN
      LOG.info("Building Virtual Network {}", TEST_VIRTUAL_NETWORK);
      azure.networks()
          .define(TEST_VIRTUAL_NETWORK)
          .withRegion(region)
          .withExistingResourceGroup(TEST_RESOURCE_GROUP)
          .withAddressSpace("10.1.0.0/24")
          .withSubnet("default", "10.1.0.0/24")
          .create();

      // User Assigned MSI
      LOG.info("Building User Assigned MSI {}", TEST_USER_ASSIGNED_MSI_NAME);
      MSIManager msiManager = credentials.getMsiManager();
      msiManager.identities()
          .define(TEST_USER_ASSIGNED_MSI_NAME)
          .withRegion(region)
          .withNewResourceGroup(TEST_RESOURCE_GROUP)
          .create();

    } catch (Exception e) {
      LOG.error("Unable to build the resource group {} and all test assets; cleaning up. Error: {}",
          TEST_RESOURCE_GROUP, e.getMessage());

      destroyLiveTestEnvironment(azure);

      throw new RuntimeException("Building resource group " + TEST_RESOURCE_GROUP +
          " failed due to " + e.getMessage());
    }

    LOG.info("Test Environment in Resource Group {} built.", TEST_RESOURCE_GROUP);
  }

  public static void destroyLiveTestEnvironment(Azure azure) {
    LOG.info("destroyLiveTestEnvironment");

    if (existingRg) {
      LOG.info("Resource Group {} was built by hand, skipping resource deletion.",
          TEST_RESOURCE_GROUP);
      return;
    }

    try {
      azure.resourceGroups().checkExistence(TEST_RESOURCE_GROUP);
    } catch (Exception e) {
      rgDeleteFailed = true;
      String errorMessage = String.format("POTENTIAL RESOURCE LEAK - unable to query Azure to " +
          "see if the resource group %s exists due to: %s", TEST_RESOURCE_GROUP, e.getMessage());
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage);
    }

    try {
      if (azure.resourceGroups().checkExistence(TEST_RESOURCE_GROUP)) {
        LOG.info("Deleting Resource Group {}", TEST_RESOURCE_GROUP);
        azure.resourceGroups().deleteByName(TEST_RESOURCE_GROUP);
      } else {
        LOG.info("Resource Group {} does not exist - skipping RG delete", TEST_RESOURCE_GROUP);
      }
    } catch (Exception e) {
      rgDeleteFailed = true;
      String errorMessage = String.format("RESOURCE LEAK - unable to delete the resource group " +
              "%s due to: %s", TEST_RESOURCE_GROUP, e.getMessage());
      LOG.error(errorMessage);
      throw new RuntimeException(errorMessage);
    }
  }
}
