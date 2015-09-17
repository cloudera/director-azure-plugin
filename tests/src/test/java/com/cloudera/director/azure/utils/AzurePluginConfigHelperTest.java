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

package com.cloudera.director.azure.utils;

import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.shaded.com.typesafe.config.Config;
import com.cloudera.director.azure.shaded.com.typesafe.config.ConfigFactory;
import com.cloudera.director.azure.shaded.com.typesafe.config.ConfigValue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


import static org.junit.Assert.assertTrue;

/**
 * Tests for AzurePluginConfigHelper class
 */
public class AzurePluginConfigHelperTest {
  private static final Logger LOG = LoggerFactory.getLogger(AzurePluginConfigHelperTest.class);

  @Test
  public void testBasicCase() throws Exception {
    // try to read and parse plugin config file
    Config cfg = AzurePluginConfigHelper.parseConfigFromClasspath(
        Configurations.AZURE_CONFIG_FILENAME);
    // verify the plugin config file
    AzurePluginConfigHelper.validatePluginConfig(cfg);
    // print the content of the config file in classpath
    for (Map.Entry<String, ConfigValue> e : cfg.entrySet()) {
      LOG.info(e.getKey() + "\t" + e.getValue());
    }
  }

  @Test
  public void testBasicErrorCase() throws Exception {
    // wrong file name
    try {
      AzurePluginConfigHelper.parseConfigFromClasspath("foobar");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("resource not found on classpath"));
    }
    // empty config
    try {
      Config cfg = ConfigFactory.empty();
      AzurePluginConfigHelper.validatePluginConfig(cfg);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("No configuration setting found"));
    }
  }

  // FIXME add a test case to verify specific sections missing. Manually testing for now.
}
