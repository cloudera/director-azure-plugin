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

import com.cloudera.director.azure.compute.instance.AzureComputeInstance;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplateConfigurationProperty;
import com.cloudera.director.azure.compute.instance.AzureInstance;
import com.cloudera.director.azure.compute.provider.AzureComputeProvider;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.Azure;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachine;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineCustomImage;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.storage.SkuName;
import com.cloudera.director.spi.v2.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v2.model.util.SimpleConfiguration;
import com.cloudera.director.spi.v2.provider.CloudProvider;
import com.cloudera.director.spi.v2.provider.Launcher;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to create and delete custom VM images. Used for testing only.
 */
public class CustomVmImageTestHelper {
  private static final Logger LOG = LoggerFactory.getLogger(CustomVmImageTestHelper.class);

  private static final String TEMPLATE_NAME = "LiveTestInstanceTemplate";
  private static final Map<String, String> TAGS = TestHelper.buildTagMap();

  private static final DefaultLocalizationContext DEFAULT_LOCALIZATION_CONTEXT =
      new DefaultLocalizationContext(Locale.getDefault(), "");

  /**
   * Create a custom managed VM image. Use the TestHelper.TEST_IMAGE_NAME (Cloudera CentOS 6.7) for
   * custom image with purchase plan and the official RHEL 7.2 for image without purchase plan.
   *
   * IMPORTANT NOTE: this test relies on SSH command (client) to prepare the VM to produce custom
   * image. The Network Security Group used for this test must allow SSH access (port 22) or the
   * created custom image will not be usable.
   *
   * @link https://docs.microsoft.com/en-us/azure/virtual-machines/linux/capture-image
   *
   * @param customImageName custom vm image name
   * @param withPlan true of create image with purchase plan
   * @return custom managed VM image URI (Resource ID)
   */
  public static String buildCustomManagedVmImage(String customImageName, boolean withPlan)
      throws Exception {
    Launcher launcher = new AzureLauncher();
    launcher.initialize(new File("non_existent_file"), null);
    CloudProvider cloudProvider = launcher.createCloudProvider(AzureCloudProvider.ID,
        TestHelper.buildValidDirectorLiveTestConfig(), Locale.getDefault());
    AzureComputeProvider provider = (AzureComputeProvider) cloudProvider.createResourceProvider(
        AzureComputeProvider.METADATA.getId(), TestHelper.buildValidDirectorLiveTestConfig());

    Map<String, String> cfgMap = TestHelper.buildValidDirectorLiveTestMap();
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.VMSIZE.unwrap().getConfigKey(),
        "STANDARD_DS1_V2");
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE.unwrap()
        .getConfigKey(), SkuName.STANDARD_LRS.toString());
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.DATA_DISK_COUNT.unwrap()
        .getConfigKey(), "0");
    // VM must have public IP and the NSG configured to allow ssh (port 22) access
    cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.PUBLIC_IP.unwrap()
        .getConfigKey(), "Yes");
    if (!withPlan) {
      // create managed custom VM image w/o purchase plan
      cfgMap.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
          TestHelper.TEST_RHEL_IMAGE_NAME);
    }

    AzureComputeInstanceTemplate template = new AzureComputeInstanceTemplate(TEMPLATE_NAME,
        new SimpleConfiguration(cfgMap), TAGS, DEFAULT_LOCALIZATION_CONTEXT);

    // the one VM to use for this test
    Collection<String> instanceIds = new ArrayList<>();
    instanceIds.add(UUID.randomUUID().toString());

    // 1. allocate the VM the first time
    LOG.info("1. allocate vm with base image");
    provider.allocate(template, instanceIds, instanceIds.size());

    // 2. verify instances can be found
    LOG.info("2. find");
    Collection<? extends AzureComputeInstance<? extends AzureInstance>> foundInstances =
        provider.find(template, instanceIds);

    // 3. de-provision VM
    LOG.info("3. de-provision");
    AzureComputeInstance<? extends AzureInstance> instance = foundInstances.iterator().next();
    VirtualMachine vm = (VirtualMachine) instance.unwrap();

    // store the key file as a temporary file for ssh command
    File keyFile = File.createTempFile("keyFile", ".tmp");
    // IMPORTANT: make sure the key file is deleted after test completes
    keyFile.deleteOnExit();
    // key file must have 600 permission
    Set<PosixFilePermission> permissions = new HashSet<>();
    permissions.add(PosixFilePermission.OWNER_READ);
    permissions.add(PosixFilePermission.OWNER_WRITE);
    Files.setPosixFilePermissions(keyFile.toPath(), permissions);
    try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(keyFile), StandardCharsets.UTF_8))) {
      bw.write(TestHelper.TEST_SSH_PRIVATE_KEY);
    }
    String sshCmd = "ssh -tt -o StrictHostKeyChecking=no -i " + keyFile.toPath() +
        " cloudera@" + vm.getPrimaryPublicIPAddress().fqdn() +
        " sudo waagent -deprovision+user --force";
    LOG.info("SSH CMD: {}", sshCmd);

    Process p = Runtime.getRuntime().exec(sshCmd);

    try (BufferedReader stdout = new BufferedReader(new InputStreamReader(
        p.getInputStream(), StandardCharsets.UTF_8));
    BufferedReader stderr = new BufferedReader(new InputStreamReader(
        p.getErrorStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = stdout.readLine()) != null) {
        LOG.info(line);
      }
      while ((line = stderr.readLine()) != null) {
        LOG.info(line);
      }
    }

    p.waitFor();

    // 4. deallocate VM
    LOG.info("4. deallocate");
    vm.deallocate();

    // 5. generalize VM
    LOG.info("5. generalize");
    vm.generalize();

    // 6. create custom image
    LOG.info("6. create custom image");
    Azure azure = TestHelper.getAzureCredentials().authenticate();
    VirtualMachineCustomImage image = azure.virtualMachineCustomImages().define(customImageName)
        .withRegion(TestHelper.TEST_REGION)
        .withExistingResourceGroup(cfgMap.get(
            AzureComputeInstanceTemplateConfigurationProperty.COMPUTE_RESOURCE_GROUP
                .unwrap().getConfigKey()))
        .fromVirtualMachine(vm)
        .create();

    LOG.info("Successfully create custom managed VM image: {}", image.name());

    // 7. delete vm
    LOG.info("7. delete vm");
    provider.delete(template, instanceIds);

    return image.id();
  }

  /**
   * Delete custom vm image.
   *
   * @param imageUri Image URI (resource ID)
   * @throws Exception when Azure fails to delete custom image
   */
  public static void deleteCustomManagedVmImage(String imageUri) throws Exception {
    LOG.info("Delete managed custom VM image: {}", imageUri);
    Azure azure = TestHelper.getAzureCredentials().authenticate();
    azure.virtualMachineCustomImages().deleteById(imageUri);
    LOG.info("Successfully deleted managed custom VM image: {}", imageUri);
  }
}
