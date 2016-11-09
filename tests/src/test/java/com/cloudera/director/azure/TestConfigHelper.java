/*
 * Copyright (c) 2016 Cloudera, Inc.
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

import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty;
import com.cloudera.director.azure.compute.provider.AzureComputeProviderConfigurationProperty;
import com.cloudera.director.azure.utils.AzureVmImageInfo;
import com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate.ComputeInstanceTemplateConfigurationPropertyToken;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Convenience functions for grabbing system properties / configuration for test.
 */
public class TestConfigHelper {
  public static final String TEST_RESOURCE_GROUP_KEY = "azure.live.rg";
  private static final String DEFAULT_TEST_RESOURCE_GROUP = "pluginUnitTestResourceGroup";
  public static final String DEFAULT_TEST_NETWORK_SECURITY_GROUP = "TestNsg";
  public static final String DEFAULT_TEST_AVAILABILITY_SET = "TestAs";
  public static final String DEFAULT_TEST_VMSIZE = "Standard_DS2";
  public static final String DEFAULT_TEST_STORAGE_ACCOUNT_TYPE =
    Configurations.AZURE_DEFAULT_STORAGE_ACCOUNT_TYPE;
  public static final String DEFAULT_TEST_DATA_DISK_COUNT = "1";
  public static final int DEFAULT_TEST_DATA_DISK_SIZE_GB =
    Configurations.AZURE_DEFAULT_DATA_DISK_SIZE;
  public static final int DEFAULT_AZURE_OPERATION_POLLING_TIMEOUT = 600;
  public static final String DEFAULT_TEST_VIRTUAL_NETWORK = "TestVnet";
  public static final String DEFAULT_TEST_SUBNET = "default";
  public static final String DEFAULT_TEST_PUBLIC_IP = "Yes";
  public static final String DEFAULT_TEST_HOST_FQDN_SUFFIX = "cdh-cluster.internal";
  public static final String DEFAULT_TEST_IMAGE = "cloudera-centos-6-latest";
  public static final String DEFAULT_TEST_REGION = "eastus";
  public static final String DEFAULT_TEST_PUBLIC_URL_POSTFIX = "cloudapp.azure.com";
  public static final List<String> DEFAULT_TEST_V1_VM_SISES = Arrays.asList(
    "Standard_DS1", "Standard_DS2", "Standard_DS3", "Standard_DS4", "Standard_DS11",
    "Standard_DS12", "Standard_DS13", "Standard_DS14");
  public static final String DEFAULT_TEST_SSH_PRIVATE_KEY =
    "-----BEGIN RSA PRIVATE KEY-----\n" +
    "MIIEpAIBAAKCAQEAsFXRzZvk4ho1SX8xMqdP+5u5iiFNm8QkfOdDI1yDP+4ISds4\n" +
    "T2Rv08v36mSExA6xKqYDFpdq8jgoOkFI69xtrm7sCDtyPy0oIwb2/3fP7N3/foAF\n" +
    "MVsVzAntycf0b4BOvvSx2VCk3zaB/o3v0vnlRnTCKk6MVX/U1ilQFIiUl4jH2sMq\n" +
    "MstYUtXEGfMuxK1ee8UCkc+B/zpMpgFpHNYUpAw7L3TL/NC7txMlD++PKI4wRecB\n" +
    "ugcSdCterxGSxhaSihx+loUzR+NBeXo7R9i5KO3U1MQ8GcRZoW/4hhEvsmmOyWq1\n" +
    "lXaRkwnzvdkPvOoccMEjKrkAqaj6JVxwt8waUQIDAQABAoIBACRefvhGWA3eU/FF\n" +
    "v5Lv+Uou0zTPK5+d89yjIjDP6u4rnSAGi/WsBHiNkCOS+eMqGJZwSSDGuDMfLATf\n" +
    "5DdpbmHU1O/ZuvWWzblzvUvxnTwAiarAotGMNNGxlo7Qo/S/ZP5zn57vyCGVr3ge\n" +
    "NEGycvx2JnntW3pi9DX7rV2e0e7y2kFeFQljLNGTKZfrqVUc4WK2WOvXjy3DHHSP\n" +
    "ImGP24tjKBpqPEMOoOup08QaWbQZf6OzFOLiA81OkAYgv31WIu3pstTKCQjz89K5\n" +
    "6ANEcqPEN0HYqFpUOyuJghPHxZzWxD98RDbAFE5OvYK4k4PlVgbDQs95ejTWcKhQ\n" +
    "Jyz8z0ECgYEA5h6w1vGJuoSX9qXL5ZitjVbUITWYRjrPN5MUwk7/4OASQ8EK8gOp\n" +
    "WPac2oUzRbaRdie+sLGFagbTBAPIgglxOiet/lWY5J6JKJHHbvAOPDnjmSCgqh3O\n" +
    "wobVSF+8aE05dzyJFMfrgjHDRVSeG9odykDYxWhiDCeEC0XJudGSzmkCgYEAxCqh\n" +
    "uXq2rNkYCObrbvWtiTikhlXS/QQRMXNyJ47SX96+OSh73mFqbj4IfjWLj2qxG6tf\n" +
    "b+xwygTevWeQPu2EEG8SMlQvymjwtJJdKRWgv3tTISGDkZ/neJ3IW4sSOCqGt8U8\n" +
    "AuNktRIqosrqpXgUO/0WrjLVIi79Q+VL2p2mP6kCgYAIggP5vm5gJfzUUqbqMy3f\n" +
    "duFa5PdfSVdV76spz+/n6YDjXmTAM+Bz+JIuBhSyNCDGpIuJTtbm55+vm1AzdpPo\n" +
    "GYV2TMXdVfsuM82SzW9JTL6cb7dg8r8tM/z4swltNcW1IdjTmtybMKnOi2VZFERb\n" +
    "sPASxFwzpNZd8FOX+iaaWQKBgQCZKPQGXmJ8iC/0218czmMEzZ3faOkINYG7C3ko\n" +
    "m6FzyvTYqdw03/h6RKLa77Gcc1/+y4oDWCckBDNozJBaIZZIQoCBnSuHLPIq+lAU\n" +
    "gNd2SbK5HnKcY64VhZPmram3ArUWjL9zPdnmal2xpx8XvK6Hu+5WakfwnaGOvlvA\n" +
    "P/CKqQKBgQDQqbITT8isN4mNNiUtv9zdzZjR6+FtZzxqGijlvfXwEb4mD6qtHDrn\n" +
    "ftMIV4nwzZ/87oXwVERCiCH/7ccS2CpXW+zzG1GswjAaxwL5/r0QJn48PZo1acXG\n" +
    "OfGEfRdtQLfDBlYqyz9lVUSddOCxrwVHrL1FbV7mJtIYAA7QfCpQyg==\n" +
    "-----END RSA PRIVATE KEY-----";
  public static final String DEFAULT_TEST_SSH_PUBLIC_KEY =
    "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCwVdHNm+TiGjVJfzEyp0/7m7mKIU" +
    "2bxCR850MjXIM/7ghJ2zhPZG/Ty/fqZITEDrEqpgMWl2ryOCg6QUjr3G2ubuwIO3I/" +
    "LSgjBvb/d8/s3f9+gAUxWxXMCe3Jx/RvgE6+9LHZUKTfNoH+je/S+eVGdMIqToxVf9" +
    "TWKVAUiJSXiMfawyoyy1hS1cQZ8y7ErV57xQKRz4H/OkymAWkc1hSkDDsvdMv80Lu3" +
    "EyUP748ojjBF5wG6BxJ0K16vEZLGFpKKHH6WhTNH40F5ejtH2Lko7dTUxDwZxFmhb/" +
    "iGES+yaY7JarWVdpGTCfO92Q+86hxwwSMquQCpqPolXHC3zBpR testUser";
  public static final String DEFAULT_TEST_SSH_USERNAME = "testUser";

  private String testResourceGroup;
  private HashMap<String, String> providerCfgMap;
  private AzureVmImageInfo defaultImageInfo;
  private AzureVmImageInfo officialRhel67ImageInfo;

  public TestConfigHelper() {
    providerCfgMap = new HashMap<String, String>();
    // pull provider configs from system
    providerCfgMap.put(AzureCredentialsConfiguration.MGMT_URL.unwrap().getConfigKey(),
      System.getProperty(
        AzureCredentialsConfiguration.MGMT_URL.unwrap().getConfigKey(),
        AzureCredentialsConfiguration.MGMT_URL.unwrap().getDefaultValue()));
    providerCfgMap.put(AzureCredentialsConfiguration.SUBSCRIPTION_ID.unwrap().getConfigKey(),
      System.getProperty(AzureCredentialsConfiguration.SUBSCRIPTION_ID.unwrap().getConfigKey()));
    providerCfgMap.put(AzureCredentialsConfiguration.AAD_URL.unwrap().getConfigKey(),
      System.getProperty(AzureCredentialsConfiguration.AAD_URL.unwrap().getConfigKey(),
      AzureCredentialsConfiguration.AAD_URL.unwrap().getDefaultValue()));
    providerCfgMap.put(AzureCredentialsConfiguration.TENANT_ID.unwrap().getConfigKey(),
      System.getProperty(AzureCredentialsConfiguration.TENANT_ID.unwrap().getConfigKey()));
    providerCfgMap.put(AzureCredentialsConfiguration.CLIENT_ID.unwrap().getConfigKey(),
      System.getProperty(AzureCredentialsConfiguration.CLIENT_ID.unwrap().getConfigKey()));
    providerCfgMap.put(AzureCredentialsConfiguration.CLIENT_SECRET.unwrap().getConfigKey(),
      System.getProperty(AzureCredentialsConfiguration.CLIENT_SECRET.unwrap().getConfigKey()));

    testResourceGroup = System.getProperty(TEST_RESOURCE_GROUP_KEY, DEFAULT_TEST_RESOURCE_GROUP);

    providerCfgMap.put(AzureComputeInstanceTemplateConfigurationProperty
      .COMPUTE_RESOURCE_GROUP.unwrap().getConfigKey(),
      testResourceGroup);
    providerCfgMap.put(AzureComputeInstanceTemplateConfigurationProperty
      .AVAILABILITY_SET.unwrap().getConfigKey(),
      DEFAULT_TEST_AVAILABILITY_SET);
    providerCfgMap.put(AzureComputeInstanceTemplateConfigurationProperty
      .VMSIZE.unwrap().getConfigKey(),
      DEFAULT_TEST_VMSIZE);
    providerCfgMap.put(AzureComputeInstanceTemplateConfigurationProperty
      .STORAGE_ACCOUNT_TYPE.unwrap().getConfigKey(),
      DEFAULT_TEST_STORAGE_ACCOUNT_TYPE);
    providerCfgMap.put(AzureComputeInstanceTemplateConfigurationProperty
      .DATA_DISK_COUNT.unwrap().getConfigKey(),
      DEFAULT_TEST_DATA_DISK_COUNT);
    providerCfgMap.put(AzureComputeInstanceTemplateConfigurationProperty
      .DATA_DISK_SIZE.unwrap().getConfigKey(),
      "" + DEFAULT_TEST_DATA_DISK_SIZE_GB);
    providerCfgMap.put(AzureComputeInstanceTemplateConfigurationProperty
      .VIRTUAL_NETWORK_RESOURCE_GROUP.unwrap().getConfigKey(),
      testResourceGroup);
    providerCfgMap.put(AzureComputeInstanceTemplateConfigurationProperty
      .VIRTUAL_NETWORK.unwrap().getConfigKey(),
      DEFAULT_TEST_VIRTUAL_NETWORK);
    providerCfgMap.put(AzureComputeInstanceTemplateConfigurationProperty
      .SUBNET_NAME.unwrap().getConfigKey(),
      DEFAULT_TEST_SUBNET);
    providerCfgMap.put(AzureComputeInstanceTemplateConfigurationProperty
      .NETWORK_SECURITY_GROUP_RESOURCE_GROUP.unwrap().getConfigKey(),
      testResourceGroup);
    providerCfgMap.put(AzureComputeInstanceTemplateConfigurationProperty
      .NETWORK_SECURITY_GROUP.unwrap().getConfigKey(),
      DEFAULT_TEST_NETWORK_SECURITY_GROUP);
    providerCfgMap.put(AzureComputeInstanceTemplateConfigurationProperty
      .PUBLIC_IP.unwrap().getConfigKey(),
      DEFAULT_TEST_PUBLIC_IP);
    providerCfgMap.put(AzureComputeInstanceTemplateConfigurationProperty
      .HOST_FQDN_SUFFIX.unwrap().getConfigKey(),
      DEFAULT_TEST_HOST_FQDN_SUFFIX);
    providerCfgMap.put(AzureComputeInstanceTemplateConfigurationProperty
      .IMAGE.unwrap().getConfigKey(),
      DEFAULT_TEST_IMAGE);
    providerCfgMap.put(ComputeInstanceTemplateConfigurationPropertyToken
      .SSH_USERNAME.unwrap().getConfigKey(),
      DEFAULT_TEST_SSH_USERNAME);
    providerCfgMap.put(ComputeInstanceTemplateConfigurationPropertyToken
      .SSH_OPENSSH_PUBLIC_KEY.unwrap().getConfigKey(),
      DEFAULT_TEST_SSH_PUBLIC_KEY);
    providerCfgMap.put(AzureComputeProviderConfigurationProperty
      .REGION.unwrap().getConfigKey(),
      DEFAULT_TEST_REGION);

    defaultImageInfo = new AzureVmImageInfo(
      "cloudera", "cloudera-centos-6", "CLOUDERA-CENTOS-6", "latest");

    officialRhel67ImageInfo = new AzureVmImageInfo(
      "RedHat", "6.7", "RHEL", "latest");
  }

  /**
   * @return Azure provider config, in Director SimpleConfiguration form.
   */
  public SimpleConfiguration getProviderConfig() {
    return new SimpleConfiguration(providerCfgMap);
  }

  /**
   * @return Azure provider config, in HashMap form.
   */
  public HashMap<String, String> getProviderCfgMap() {
    return new HashMap<String, String>(providerCfgMap);
  }

  /**
   * @return true if run live test flag is set.
   */
  public static boolean runLiveTests() {
    String liveString = System.getProperty("test.azure.live");
    return Boolean.parseBoolean(liveString);
  }

  /**
   * @return the resource group to be used by live tests.
   */
  public String getTestResourceGroup() {
    return this.testResourceGroup;
  }

  /**
   * Create a AzureCredentials object using stored credentials.
   *
   * @return an AzureCredentials object.
   */
  public AzureCredentials getAzureCredentials() {
    return new AzureCredentials(
      providerCfgMap.get(AzureCredentialsConfiguration.SUBSCRIPTION_ID.unwrap().getConfigKey()),
      providerCfgMap.get(AzureCredentialsConfiguration.MGMT_URL.unwrap().getConfigKey()),
      providerCfgMap.get(AzureCredentialsConfiguration.AAD_URL.unwrap().getConfigKey()),
      providerCfgMap.get(AzureCredentialsConfiguration.TENANT_ID.unwrap().getConfigKey()),
      providerCfgMap.get(AzureCredentialsConfiguration.CLIENT_ID.unwrap().getConfigKey()),
      providerCfgMap.get(AzureCredentialsConfiguration.CLIENT_SECRET.unwrap().getConfigKey()));
  }

  /**
   * @return the default VM image info (Cloudera CentOS 6)
   */
  public AzureVmImageInfo getDefaultImageInfo() {
    return this.defaultImageInfo;
  }

  /**
   * @return the official RHEL 6.7 image info
   */
  public AzureVmImageInfo getOfficialRhel67ImageInfo() { return this.officialRhel67ImageInfo; }
}
