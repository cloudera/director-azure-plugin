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

import com.cloudera.director.azure.TestConfigHelper;
import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v1.model.ConfigurationValidator;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v1.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.cloudera.director.azure.shaded.com.typesafe.config.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static com.cloudera.director.azure.Configurations.AZURE_CONFIG_PROVIDER;
import static org.junit.Assert.assertTrue;

/**
 * Simple test to verify AzureComputeProviderConfigurationValidatorTest class.
 */
public class AzureComputeProviderConfigurationValidatorTest {
  private Config pluginConfig;
  private LocalizationContext localizationContext;
  private TestConfigHelper cfgHelper = new TestConfigHelper();
  private ConfigurationValidator validator;
  private PluginExceptionConditionAccumulator accumulator;

  @Before
  public void setUp() throws Exception {
    pluginConfig = AzurePluginConfigHelper.parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME);
    localizationContext= new DefaultLocalizationContext(Locale.getDefault(), "");
    validator = new AzureComputeProviderConfigurationValidator(pluginConfig.getConfig(AZURE_CONFIG_PROVIDER));
    accumulator = new PluginExceptionConditionAccumulator();
  }

  @After
  public void tearDown() throws Exception {
    pluginConfig = null;
    localizationContext= null;
    validator = null;
    accumulator = null;
  }

  @Test
  public void testValidRegionConfig() throws Exception {
    // All should be well
    validator.validate(null, cfgHelper.getProviderConfig(), accumulator, localizationContext);
    assertTrue(accumulator.getConditionsByKey().isEmpty());
  }

  @Test
  public void testInvalidRegionConfig() throws Exception {
    // build a config with bogus region
    Map<String, String> cfgMap = new HashMap<String, String>();
    cfgMap.put("region", "foobar");

    // validator should catch the error
    validator.validate(null, new SimpleConfiguration(cfgMap), accumulator, localizationContext);
    assertTrue(accumulator.getConditionsByKey().size() == 1);
  }
}
