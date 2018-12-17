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

import com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration;
import com.cloudera.director.azure.compute.provider.AzureComputeProvider;
import com.cloudera.director.azure.shaded.com.microsoft.azure.AzureEnvironment;
import com.cloudera.director.azure.shaded.com.typesafe.config.ConfigFactory;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v2.model.exception.InvalidCredentialsException;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v2.provider.CloudProvider;
import com.cloudera.director.spi.v2.provider.Launcher;
import com.cloudera.director.spi.v2.provider.ResourceProviderMetadata;

import java.io.File;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/**
 * AzureCloudProvider tests.
 */
public class AzureCloudProviderLiveTest {

  private Launcher launcher;

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void checkLiveTestFlag() {
    Assume.assumeTrue(TestHelper.runLiveTests());
  }

  @Before
  public void setUp() throws Exception {
    launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
  }

  @After
  public void reset() throws Exception {
    TestHelper.setAzurePluginConfigNull();
    TestHelper.setConfigurableImagesNull();
  }

  @Test
  public void createCloudProviderWithInvalidDeprecatedManagementUrlsExpectExceptionThrown()
      throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureCredentialsConfiguration.MGMT_URL.unwrap().getConfigKey(),
        "https://fake-management.azure.com/");

    thrown.expect(InvalidCredentialsException.class);
    launcher.createCloudProvider(AzureCloudProvider.ID,
        new SimpleConfiguration(map), Locale.getDefault());
  }

  @Test
  public void createResourceProviderWithValidCredentialsExpectSuccess() throws Exception {
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());

    cloudProvider.createResourceProvider(AzureComputeProvider.METADATA.getId(),
        TestHelper.buildValidDirectorLiveTestConfig());
  }

  @Test
  public void createResourceProviderWithDeprecatedManagementUrlsExpectSuccess() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureCredentialsConfiguration.MGMT_URL.unwrap().getConfigKey(),
        AzureEnvironment.AZURE.managementEndpoint());

    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        new SimpleConfiguration(map), Locale.getDefault());

    cloudProvider.createResourceProvider(AzureComputeProvider.METADATA.getId(),
        TestHelper.buildValidDirectorLiveTestConfig());
  }

  @Test
  public void createResourceProviderWithInvalidCredentialsExpectSuccess() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureCredentialsConfiguration.CLIENT_SECRET.unwrap().getConfigKey(),
        "fake-client-secret");

    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        new SimpleConfiguration(map), Locale.getDefault());

    cloudProvider.createResourceProvider(AzureComputeProvider.METADATA.getId(),
        TestHelper.buildValidDirectorLiveTestConfig());
  }

  @Test
  public void createResourceProviderWithInvalidIdExpectExceptionThrown() throws Exception {
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());

    thrown.expect(NoSuchElementException.class);
    cloudProvider.createResourceProvider("invalid-ID",
        TestHelper.buildValidDirectorLiveTestConfig());
  }

  @Test
  public void createResourceProviderWithInvalidCredentialsDoNotValidateExpectSuccess()
      throws Exception {
    TestHelper.setAzurePluginConfigNull();
    Map<String, Object> pluginMap = new HashMap<>();
    pluginMap.put(Configurations.AZURE_VALIDATE_CREDENTIALS, "false");
    AzurePluginConfigHelper.setAzurePluginConfig(ConfigFactory.parseMap(pluginMap));

    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureCredentialsConfiguration.CLIENT_SECRET.unwrap().getConfigKey(),
        "fake-client-secret");

    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        new SimpleConfiguration(map), Locale.getDefault());

    cloudProvider.createResourceProvider(AzureComputeProvider.METADATA.getId(),
        TestHelper.buildValidDirectorLiveTestConfig());
  }

  @Test
  public void getResourceProviderConfigurationValidatorWithValidMetadataExpectSuccess()
      throws Exception {
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());

    ResourceProviderMetadata resourceProviderMetadata =
        Mockito.mock(ResourceProviderMetadata.class);
    Mockito.when(resourceProviderMetadata.getId())
        .thenReturn(AzureComputeProvider.METADATA.getId());

    AzureCloudProvider azureCloudProvider = (AzureCloudProvider) cloudProvider;
    azureCloudProvider.getResourceProviderConfigurationValidator(resourceProviderMetadata);
  }

  @Test
  public void
  getResourceProviderConfigurationValidatorWithInvalidMetadataExpectExceptionThrown()
      throws Exception {
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());

    ResourceProviderMetadata resourceProviderMetadata =
        Mockito.mock(ResourceProviderMetadata.class);
    Mockito.when(resourceProviderMetadata.getId()).thenReturn("invalid-ID");

    AzureCloudProvider azureCloudProvider = (AzureCloudProvider) cloudProvider;
    thrown.expect(NoSuchElementException.class);
    azureCloudProvider.getResourceProviderConfigurationValidator(resourceProviderMetadata);
  }

  @Test
  public void
  getResourceProviderConfigurationValidatorWithInvalidMetadataDoNotValidateExpectSuccess()
      throws Exception {
    TestHelper.setAzurePluginConfigNull();
    Map<String, Object> pluginMap = new HashMap<>();
    pluginMap.put(Configurations.AZURE_VALIDATE_RESOURCES, "false");
    AzurePluginConfigHelper.setAzurePluginConfig(ConfigFactory.parseMap(pluginMap));

    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());

    ResourceProviderMetadata resourceProviderMetadata =
        Mockito.mock(ResourceProviderMetadata.class);
    Mockito.when(resourceProviderMetadata.getId()).thenReturn("invalid-ID");

    AzureCloudProvider azureCloudProvider = (AzureCloudProvider) cloudProvider;
    azureCloudProvider.getResourceProviderConfigurationValidator(resourceProviderMetadata);
  }
}
