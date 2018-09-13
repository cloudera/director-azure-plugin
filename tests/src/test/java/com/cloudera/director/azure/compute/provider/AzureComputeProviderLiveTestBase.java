/*
 * Copyright (c) 2018 Cloudera, Inc.
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
 *
 */

package com.cloudera.director.azure.compute.provider;

import com.cloudera.director.azure.AzureLauncher;
import com.cloudera.director.azure.TestHelper;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.Azure;
import com.cloudera.director.spi.v2.provider.Launcher;

import java.io.File;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for live test.
 */
public class AzureComputeProviderLiveTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(AzureComputeProviderLiveTestBase.class);

  static AzureCredentials credentials;
  static Azure azure;
  protected static final Launcher LAUNCHER = new AzureLauncher();

  @BeforeClass
  public static void createLiveTestResources() throws Exception {
    // initialize everything only if live check passes
    LAUNCHER.initialize(new File("non_existent_file"), null);

    credentials = TestHelper.getAzureCredentials();
    azure = credentials.authenticate();

    LOG.info("createLiveTestResources");

    Assume.assumeTrue(TestHelper.runLiveTests());
    TestHelper.buildLiveTestEnvironment(credentials);
  }

  @AfterClass
  public static void destroyLiveTestResources() throws Exception {
    LOG.info("destroyLiveTestResources");
    // this method is always called

    // destroy everything only if live check passes
    if (TestHelper.runLiveTests()) {
      TestHelper.destroyLiveTestEnvironment(azure);
    }
  }
}
