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

import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.TestHelper;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v2.model.ConfigurationValidator;
import com.cloudera.director.spi.v2.model.LocalizationContext;
import com.cloudera.director.spi.v2.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v2.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;

import java.util.Locale;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * AzureComputeProviderConfigurationValidator tests.
 */
public class AzureComputeProviderConfigurationValidatorTest {

  // Fields used by checks
  private ConfigurationValidator validator;
  private PluginExceptionConditionAccumulator accumulator;
  private LocalizationContext localizationContext;

  @Before
  public void setUp() throws Exception {
    // Reset the plugin config with the default config.
    AzurePluginConfigHelper.setAzurePluginConfig(AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME));
    AzurePluginConfigHelper.setConfigurableImages(AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIGURABLE_IMAGES_FILE));

    validator = new AzureComputeProviderConfigurationValidator();
    accumulator = new PluginExceptionConditionAccumulator();
    localizationContext = new DefaultLocalizationContext(Locale.getDefault(), "");
  }

  @After
  public void reset() throws Exception {
    accumulator.getConditionsByKey().clear();

    TestHelper.setAzurePluginConfigNull();
    TestHelper.setConfigurableImagesNull();
  }

  @Test
  public void validateWithDefaultsExpectSuccess() throws Exception {
    validator.validate(null, TestHelper.buildValidDirectorLiveTestConfig(), accumulator,
        localizationContext);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void validateWithInvalidRegionExpectAccumulatesErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeProviderConfigurationProperty.REGION.unwrap().getConfigKey(),
        "fake-region");

    validator.validate(null, new SimpleConfiguration(map), accumulator,
        localizationContext);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }
}
