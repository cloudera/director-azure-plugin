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
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.azure.shaded.com.typesafe.config.Config;
import com.cloudera.director.spi.v1.model.exception.InvalidCredentialsException;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v1.provider.CloudProvider;
import com.cloudera.director.spi.v1.provider.Launcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.doReturn;

/**
 * Local Unit Tests + One Live Test to verify AzureLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Configurations.class, AzurePluginConfigHelper.class})
public class AzureLauncherTest {

  private static final Logger LOG = LoggerFactory.getLogger(AzureLauncherTest.class);
  Launcher launcher;
  Map cfgMap;

  @Before
  public void setUp(){
    PowerMockito.spy(AzurePluginConfigHelper.class);
    launcher = new AzureLauncher();
    launcher.initialize(null, null);
    cfgMap = new TestConfigHelper().getProviderCfgMap();
    cfgMap.put(AzureCredentialsConfiguration.SUBSCRIPTION_ID.unwrap().getConfigKey(),
      "NO_A_SUB_ID");
    cfgMap.put(AzureCredentialsConfiguration.CLIENT_ID.unwrap().getConfigKey(),
      "NO_A_CLIENT_ID");
    cfgMap.put(AzureCredentialsConfiguration.CLIENT_SECRET.unwrap().getConfigKey(),
      "NO_A_CLIENT_SECRET");
    cfgMap.put(AzureCredentialsConfiguration.TENANT_ID.unwrap().getConfigKey(),
      "NO_A_TENANT_ID");
    PowerMockito.spy(Configurations.class);
  }

  /**
   * This test verifies credential checks are skipped, we have a live test that verifies
   * AzureLauncher executing the credential checks with backend
   */
  @Test
  public void testCreateCloudProviderWithoutCredentialCheck() throws Exception {
    doReturn(false).when(Configurations.class, "getValidateCredentialsFlag", any(Config.class));
    CloudProvider cloudProvider = launcher.createCloudProvider(
      AzureCloudProvider.ID,
      new SimpleConfiguration(cfgMap),
      Locale.getDefault());
    assertEquals(AzureCloudProvider.class, cloudProvider.getClass());
  }

  @Test
  public void initialize_anyArguments_callsValidate() throws Exception {
    // verify that validatePluginConfig was called
    PowerMockito.verifyStatic();
    AzurePluginConfigHelper.validatePluginConfig(any(Config.class));
  }

  /**
   * WARNING
   * This LIVE test verifies credential checks is being invoked.
   * PowerMock has a bug that will ignore the assumeTrue method. Therefore, this test is being
   * placed here.
   */
  @Test
  public void testCreateCloudProviderThrowsExceptionDuringCredentialCheck() throws Exception {
    if (!TestConfigHelper.runLiveTests()) {
      LOG.info("Skipping the test case because Live Test flag is OFF.");
      return;
    }
    doReturn(true).when(Configurations.class, "getValidateCredentialsFlag", any(Config.class));

    // WARNING This actually reaches out to Azure backend
    try {
      launcher.createCloudProvider(AzureCloudProvider.ID, new SimpleConfiguration(cfgMap),
      Locale.getDefault());
    } catch (InvalidCredentialsException e) {
      LOG.info("Caught InvalidCredentialsException as expected");
    }
  }
}
