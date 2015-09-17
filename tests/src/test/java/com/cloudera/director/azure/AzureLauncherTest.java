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
import com.cloudera.director.spi.v1.model.ConfigurationProperty;
import com.cloudera.director.spi.v1.provider.CloudProvider;
import com.cloudera.director.spi.v1.provider.CloudProviderMetadata;
import com.cloudera.director.spi.v1.provider.Launcher;
import org.junit.Test;

import java.util.List;
import java.util.Locale;

import static com.cloudera.director.azure.compute.credentials.AzureCredentialsConfiguration.*;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Tests to verify AzureLauncher.
 */
public class AzureLauncherTest {

  @Test
  public void testCreateCloudProvider() throws Exception {
    // skip the test if live test flag is not set
    assumeTrue(TestConfigHelper.runLiveTests());

    Launcher launcher = new AzureLauncher();

    assertEquals(1, launcher.getCloudProviderMetadata().size());
    CloudProviderMetadata metadata = launcher.getCloudProviderMetadata().get(0);

    assertEquals(AzureCloudProvider.ID, metadata.getId());

    List<ConfigurationProperty> credentialsConfigProperties =
        metadata.getCredentialsProviderMetadata().getCredentialsConfigurationProperties();
    // See AzureCredentialsConfiguration
    assertEquals(AzureCredentialsConfiguration.values().length, credentialsConfigProperties.size());
    assertTrue(credentialsConfigProperties.contains(MGMT_URL.unwrap()));
    assertTrue(credentialsConfigProperties.contains(SUBSCRIPTION_ID.unwrap()));
    assertTrue(credentialsConfigProperties.contains(AAD_URL.unwrap()));
    assertTrue(credentialsConfigProperties.contains(TENANT_ID.unwrap()));
    assertTrue(credentialsConfigProperties.contains(CLIENT_ID.unwrap()));
    assertTrue(credentialsConfigProperties.contains(CLIENT_SECRET.unwrap()));

    // Pull credentials from the "provider" section in sample config
    TestConfigHelper cfgHelper = new TestConfigHelper();

    // WARNING This actually reaches out to Azure backend
    CloudProvider cloudProvider = launcher.createCloudProvider(
        AzureCloudProvider.ID,
        cfgHelper.getProviderConfig(),
        Locale.getDefault());
    assertEquals(AzureCloudProvider.class, cloudProvider.getClass());
  }
}
