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

package com.cloudera.director.azure.compute.provider;

import static org.junit.Assert.assertEquals;

import com.cloudera.director.azure.AzureCloudProvider;
import com.cloudera.director.azure.AzureLauncher;
import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.TestHelper;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationValidator;
import com.cloudera.director.azure.shaded.com.typesafe.config.ConfigFactory;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v1.model.util.DefaultConfigurationValidator;
import com.cloudera.director.spi.v1.provider.CloudProvider;
import com.cloudera.director.spi.v1.provider.Launcher;

import java.io.File;
import java.util.Locale;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

/**
 * AzureComputeProvider tests.
 */
public class AzureComputeProviderTest {

  @After
  public void reset() throws Exception {
    TestHelper.setAzurePluginConfigNull();
    TestHelper.setConfigurableImagesNull();
  }

  @Test
  public void getResourceTemplateConfigurationValidatorWithValidateResourcesFalseReturnsDCV()
      throws Exception {
    // set up the custom plugin config with the validate resources flag false
    Map<String, Object> map = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME).root().unwrapped();
    map.put(Configurations.AZURE_VALIDATE_RESOURCES, "false"); // subject of the test
    map.put(Configurations.AZURE_VALIDATE_CREDENTIALS, "false"); // needed because this is a UT
    AzurePluginConfigHelper.setAzurePluginConfig(ConfigFactory.parseMap(map));

    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    assertEquals(DefaultConfigurationValidator.class,
        provider.getResourceTemplateConfigurationValidator().getClass());
  }

  @Test
  public void getResourceTemplateConfigurationValidatorWithValidateResourcesTrueReturnsACITCV()
      throws Exception {
    // set up the custom plugin config, by default the validate resources flag is true
    Map<String, Object> map = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME).root().unwrapped();
    map.put(Configurations.AZURE_VALIDATE_CREDENTIALS, "false"); // needed because this is a UT
    AzurePluginConfigHelper.setAzurePluginConfig(ConfigFactory.parseMap(map));

    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null); // so we default to azure-plugin.conf
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    assertEquals(AzureComputeInstanceTemplateConfigurationValidator.class,
        provider.getResourceTemplateConfigurationValidator().getClass());
  }
}
