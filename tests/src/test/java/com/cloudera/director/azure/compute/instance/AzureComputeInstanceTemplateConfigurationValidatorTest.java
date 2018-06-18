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

package com.cloudera.director.azure.compute.instance;

import com.cloudera.director.azure.Configurations;
import com.cloudera.director.azure.TestHelper;
import com.cloudera.director.azure.compute.credentials.AzureCredentials;
import com.cloudera.director.azure.shaded.com.microsoft.azure.Page;
import com.cloudera.director.azure.shaded.com.microsoft.azure.PagedList;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.Azure;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.PurchasePlan;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineCustomImage;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineCustomImages;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineImage;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineImages;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachinePublisher;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachinePublishers;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.resources.fluentcore.arm.Region;
import com.cloudera.director.azure.shaded.com.microsoft.azure.management.storage.SkuName;
import com.cloudera.director.azure.shaded.com.microsoft.rest.RestException;
import com.cloudera.director.azure.shaded.com.typesafe.config.Config;
import com.cloudera.director.azure.shaded.com.typesafe.config.ConfigFactory;
import com.cloudera.director.azure.utils.AzurePluginConfigHelper;
import com.cloudera.director.spi.v1.compute.ComputeInstanceTemplate;
import com.cloudera.director.spi.v1.model.InstanceTemplate;
import com.cloudera.director.spi.v1.model.LocalizationContext;
import com.cloudera.director.spi.v1.model.exception.PluginExceptionConditionAccumulator;
import com.cloudera.director.spi.v1.model.exception.ValidationException;
import com.cloudera.director.spi.v1.model.util.DefaultLocalizationContext;
import com.cloudera.director.spi.v1.model.util.SimpleConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AzureComputeInstanceTemplateConfigurationValidator tests for checks that don't require
 * interacting with the Azure backend.
 *
 * Scenarios purposefully not tested:
 * - missing template fields that are required: Director does an initial template validation pass
 *   that will catch missing fields
 */
public class AzureComputeInstanceTemplateConfigurationValidatorTest {

  private static final Logger LOG = LoggerFactory.getLogger(
      AzureComputeInstanceTemplateConfigurationValidatorTest.class);

  // Map the fields to easy to reference Strings
  private static final String IMAGE = AzureComputeInstanceTemplateConfigurationProperty.IMAGE
      .unwrap().getConfigKey();
  private static final String SSH_USERNAME = ComputeInstanceTemplate
      .ComputeInstanceTemplateConfigurationPropertyToken.SSH_USERNAME.unwrap().getConfigKey();
  private static final String INSTANCE_NAME_PREFIX = InstanceTemplate
      .InstanceTemplateConfigurationPropertyToken.INSTANCE_NAME_PREFIX.unwrap().getConfigKey();
  private static final String DATA_DISK_SIZE = AzureComputeInstanceTemplateConfigurationProperty
      .DATA_DISK_SIZE.unwrap().getConfigKey();
  private static final String HOST_FQDN_SUFFIX = AzureComputeInstanceTemplateConfigurationProperty
      .HOST_FQDN_SUFFIX.unwrap().getConfigKey();
  private static final String STORAGE_ACCOUNT_TYPE =
      AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE.unwrap()
          .getConfigKey();

  // Fields used by checks
  private AzureCredentials credentials;
  private AzureComputeInstanceTemplateConfigurationValidator validator;
  private PluginExceptionConditionAccumulator accumulator;
  private LocalizationContext localizationContext;
  private Azure azure;

  @Before
  public void setUp() throws Exception {
    // Reset the plugin config with the default config.
    AzurePluginConfigHelper.setAzurePluginConfig(AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME));
    AzurePluginConfigHelper.setConfigurableImages(AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIGURABLE_IMAGES_FILE));

    credentials = Mockito.mock(AzureCredentials.class);
    validator = new AzureComputeInstanceTemplateConfigurationValidator(credentials,
        TestHelper.TEST_REGION);
    accumulator = new PluginExceptionConditionAccumulator();
    localizationContext = new DefaultLocalizationContext(Locale.getDefault(), "");
    azure = Mockito.mock(Azure.class);

    // Set up mocks for image tests
    VirtualMachineImages vmImages = Mockito.mock(VirtualMachineImages.class);
    VirtualMachineImage vmImage = Mockito.mock(VirtualMachineImage.class);
    VirtualMachinePublishers vmPublishers = Mockito.mock(VirtualMachinePublishers.class);
    Mockito.when(azure.virtualMachineImages()).thenReturn(vmImages);
    Mockito.when(vmImages.getImage(ArgumentMatchers.any(Region.class), ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
        .thenReturn(vmImage);
    Mockito.when(vmImages.publishers()).thenReturn(vmPublishers);
    PagedList<VirtualMachinePublisher> vmPublishersList = new PagedList<VirtualMachinePublisher>() {
      @Override
      public Page<VirtualMachinePublisher> nextPage(String nextPageLink)
          throws RestException, IOException {
        return null;
      }
    };
    Mockito.when(vmPublishers.listByRegion(ArgumentMatchers.anyString()))
        .thenReturn(vmPublishersList);
  }

  @After
  public void reset() throws Exception {
    accumulator.getConditionsByKey().clear();

    TestHelper.setAzurePluginConfigNull();
    TestHelper.setConfigurableImagesNull();
  }

  @Test
  public void checkFQDNSuffixWithValidTemplateExpectNoError() throws Exception {
    validator.checkFQDNSuffix(TestHelper.buildValidDirectorUnitTestConfig(), accumulator,
        localizationContext);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkFQDNSuffixNullExpectNoError() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(HOST_FQDN_SUFFIX, null);

    validator.checkFQDNSuffix(new SimpleConfiguration(map), accumulator, localizationContext);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkFQDNSuffixEmptyStringExpectNoError() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(HOST_FQDN_SUFFIX, "");

    validator.checkFQDNSuffix(new SimpleConfiguration(map), accumulator, localizationContext);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkFQDNSuffixWithValidLengthsExpectNoError() throws Exception {
    final List<String> suffixes = new ArrayList<>();
    // It can be >= 1 character
    suffixes.add("a");
    suffixes.add("1");
    suffixes.add("ab");
    suffixes.add("12");
    suffixes.add("abc");
    suffixes.add("123");
    // It can be <= 37 characters
    suffixes.add("aaaaaaaaaaaaaaaa.cdh-cluster.internal"); // 37 characters

    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();

    for (String suffix : suffixes) {
      map.put(HOST_FQDN_SUFFIX, suffix);

      validator.checkFQDNSuffix(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(0, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkFQDNSuffixWithInvalidLengthsExpectAccumulatesErrors() throws Exception {
    final List<String> suffixes = new ArrayList<>();
    // It cannot be > 37 characters
    suffixes.add("aaaaaaaaaaaaaaaaa.cdh-cluster.internal"); // 38 characters

    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();

    for (String suffix : suffixes) {
      map.put(HOST_FQDN_SUFFIX, suffix);

      validator.checkFQDNSuffix(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(1, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkFQDNSuffixWithHyphensAndNumbersAndDotsInValidPositionsExpectNoError()
      throws Exception {
    final List<String> suffixes = new ArrayList<>();
    suffixes.add("aaaa"); // only letters
    suffixes.add("1111"); // only numbers
    suffixes.add("a-a"); // contains a hyphen
    suffixes.add("a-----a"); // contains multiple hyphens
    suffixes.add("a1a"); // contains a number
    suffixes.add("a111111a"); // contains multiple numbers
    suffixes.add("1aa"); // starts with a number
    suffixes.add("a1"); // ends with a number
    suffixes.add("aa1"); // ends with a number
    suffixes.add("aa111111"); // ends with multiple numbers
    suffixes.add("abcde.a.abcde"); // length of 1
    suffixes.add("abcde.ab.abcde"); // length of 2
    suffixes.add("abcde.abc.abcde"); // length of 3
    suffixes.add("abcde.1.abcde"); // only numbers, length of 1
    suffixes.add("abcde.12.abcde"); // only numbers, length of 2
    suffixes.add("abcde.123.abcde"); // only numbers, length of 3
    suffixes.add("abcd.1abc.abcd"); // starts with number
    suffixes.add("aaa-bbb-ccc-ddd-eee-fff-ggg-hhh-iii"); // contains lots of hyphens (-)
    suffixes.add("aaa.bbb.ccc.ddd.eee.fff.ggg.hhh.iii"); // contains lots of dots (.)
    suffixes.add("aaa.bb2.c3c.d44.e-e5.f--f.ggg.hhh.iii"); // contains lots of everything

    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();

    for (String suffix : suffixes) {
      map.put(HOST_FQDN_SUFFIX, suffix);

      validator.checkFQDNSuffix(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(0, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkFQDNSuffixWithHyphensAndNumbersAndDotsInInvalidPositionsExpectAccumulatesErrors()
      throws Exception {
    final List<String> suffixes = new ArrayList<>();
    // It can contain only lowercase letters, numbers and hyphens.
    // The first character must be a letter.
    // The last character must be a letter or number.
    suffixes.add("-abc"); // starts with hyphen
    suffixes.add("abc-"); // ends with hyphen
    suffixes.add("ABC"); // not lowercase
    suffixes.add("aBc"); // not lowercase
    suffixes.add("Abc"); // not lowercase
    suffixes.add("abC"); // not lowercase
    // Same as above, but wrapped with valid labels.
    suffixes.add("abcd..abcd"); // empty
    suffixes.add("abcd.-abc.abcd"); // starts with hyphen
    suffixes.add("abcd.abc-.abcd"); // ends with hyphen
    suffixes.add("abcd.ABC.abcd"); // not lowercase
    suffixes.add("abcd.aBc.abcd"); // not lowercase
    suffixes.add("abcd.Abc.abcd"); // not lowercase
    suffixes.add("abcd.abC.abcd"); // not lowercase
    // Cannot start or end with dot
    suffixes.add(".abc");
    suffixes.add("abc.");

    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();

    for (String suffix : suffixes) {
      map.put(HOST_FQDN_SUFFIX, suffix);

      validator.checkFQDNSuffix(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(1, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkFQDNSuffixWithInvalidCharactersExpectAccumulatesErrors() throws Exception {
    final List<String> suffixes = new ArrayList<>();
    // It cannot contain the following characters:
    // ` ~ ! @ # $ % ^ & * ( ) = + _ [ ] { } \ | ; : ' " , < > / ?
    suffixes.add("ab`cd");
    suffixes.add("ab~cd");
    suffixes.add("ab!cd");
    suffixes.add("ab@cd");
    suffixes.add("ab#cd");
    suffixes.add("ab$cd");
    suffixes.add("ab%cd");
    suffixes.add("ab^cd");
    suffixes.add("ab&cd");
    suffixes.add("ab*cd");
    suffixes.add("ab(cd");
    suffixes.add("ab)cd");
    suffixes.add("ab=cd");
    suffixes.add("ab+cd");
    suffixes.add("ab_cd");
    suffixes.add("ab[cd");
    suffixes.add("ab]cd");
    suffixes.add("ab{cd");
    suffixes.add("ab}cd");
    suffixes.add("ab\\cd");
    suffixes.add("ab|cd");
    suffixes.add("ab;cd");
    suffixes.add("ab:cd");
    suffixes.add("ab'cd");
    suffixes.add("ab\"cd");
    suffixes.add("ab,cd");
    suffixes.add("ab<cd");
    suffixes.add("ab>cd");
    suffixes.add("ab/cd");
    suffixes.add("ab?cd");

    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();

    for (String suffix : suffixes) {
      map.put(HOST_FQDN_SUFFIX, suffix);

      validator.checkFQDNSuffix(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(1, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkInstancePrefixWithValidTemplateExpectNoError() throws Exception {
    validator.checkInstancePrefix(TestHelper.buildValidDirectorUnitTestConfig(), accumulator,
        localizationContext);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkInstancePrefixWithMissingTemplateUsesDefaultExpectNoError() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.remove(INSTANCE_NAME_PREFIX);

    validator.checkInstancePrefix(new SimpleConfiguration(map), accumulator, localizationContext);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkInstancePrefixWithValidLengthsExpectNoError() throws Exception {
    final List<String> prefixes = new ArrayList<>();
    // It can be >= 1 characters
    prefixes.add("a");
    prefixes.add("aa");
    prefixes.add("aaa");
    prefixes.add("aaaa");
    // It can be <= 17 characters
    prefixes.add("aaaaaaaaaaaaaaaa"); // 15 characters
    prefixes.add("aaaaaaaaaaaaaaaaa"); // 16 characters

    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();

    for (String prefix : prefixes) {
      map.put(INSTANCE_NAME_PREFIX, prefix);

      validator.checkInstancePrefix(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(0, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkInstancePrefixWithInvalidLengthsExpectAccumulatesErrors() throws Exception {
    final List<String> prefixes = new ArrayList<>();
    // It cannot be < 1 characters
    prefixes.add("");
    // It cannot be > 17 characters
    prefixes.add("aaaaaaaaaaaaaaaaaa"); // 18 characters

    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();

    for (String prefix : prefixes) {
      map.put("instanceNamePrefix", prefix);

      validator.checkInstancePrefix(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(1, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkInstancePrefixWithHyphensAndNumbersInValidPositionsExpectNoError()
      throws Exception {
    final List<String> prefixes = new ArrayList<>();
    // hyphens
    prefixes.add("a-a");
    prefixes.add("aaa-aaa-aaa-aaa");
    prefixes.add("a-----a");
    // numbers
    prefixes.add("a1");
    prefixes.add("a11");
    prefixes.add("a1a");
    prefixes.add("a111111a");
    prefixes.add("aaa1aaa1aaa1aaa");
    prefixes.add("aa1");
    prefixes.add("aaa1aaa1aaa1aaa1");
    prefixes.add("aa111111");
    // both
    prefixes.add("a-1");
    prefixes.add("a1-1");
    prefixes.add("a1-a");
    prefixes.add("a-a1-1-a11");

    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();

    for (String prefix : prefixes) {
      map.put(INSTANCE_NAME_PREFIX, prefix);

      validator.checkInstancePrefix(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(0, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkInstancePrefixWithHyphensAndNumbersInInvalidPositionsExpectAccumulatesErrors()
      throws Exception {
    final List<String> prefixes = new ArrayList<>();
    // It can contain only lowercase letters, numbers and hyphens.
    // The first character must be a letter.
    // The last character must be a letter or number.
    prefixes.add("1"); // only a number
    prefixes.add("-"); // only a hyphen
    prefixes.add("1abc"); // starts with number
    prefixes.add("-abc"); // starts with hyphen
    prefixes.add("abc-"); // ends with hyphen
    prefixes.add("ABC"); // not lowercase
    prefixes.add("aBc"); // not lowercase
    prefixes.add("Abc"); // not lowercase
    prefixes.add("abC"); // not lowercase

    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();

    for (String prefix : prefixes) {
      map.put(INSTANCE_NAME_PREFIX, prefix);

      validator.checkInstancePrefix(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(1, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkInstancePrefixWithInvalidCharactersExpectAccumulatesErrors() throws Exception {
    final List<String> prefixes = new ArrayList<>();
    // It cannot contain the following characters:
    // ` ~ ! @ # $ % ^ & * ( ) = + _ [ ] { } \ | ; : ' " , < > / ?
    prefixes.add("ab`cd");
    prefixes.add("ab~cd");
    prefixes.add("ab!cd");
    prefixes.add("ab@cd");
    prefixes.add("ab#cd");
    prefixes.add("ab$cd");
    prefixes.add("ab%cd");
    prefixes.add("ab^cd");
    prefixes.add("ab&cd");
    prefixes.add("ab*cd");
    prefixes.add("ab(cd");
    prefixes.add("ab)cd");
    prefixes.add("ab=cd");
    prefixes.add("ab+cd");
    prefixes.add("ab_cd");
    prefixes.add("ab[cd");
    prefixes.add("ab]cd");
    prefixes.add("ab{cd");
    prefixes.add("ab}cd");
    prefixes.add("ab\\cd");
    prefixes.add("ab|cd");
    prefixes.add("ab;cd");
    prefixes.add("ab:cd");
    prefixes.add("ab'cd");
    prefixes.add("ab\"cd");
    prefixes.add("ab,cd");
    prefixes.add("ab<cd");
    prefixes.add("ab>cd");
    prefixes.add("ab/cd");
    prefixes.add("ab?cd");

    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();

    for (String prefix : prefixes) {
      map.put(INSTANCE_NAME_PREFIX, prefix);

      validator.checkInstancePrefix(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(1, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkStorageWithValidTemplateExpectNoError() throws Exception {
    validator.checkStorage(TestHelper.buildValidDirectorUnitTestConfig(), accumulator,
        localizationContext);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkStorageWithDeprecatedValuesExpectNoError() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();

    for (String type : Configurations.DEPRECATED_STORAGE_ACCOUNT_TYPES.keySet()) {
      map.put(AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE.unwrap()
          .getConfigKey(), type);

      validator.checkStorage(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(0, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkStorageWithInvalidValuesExpectNoError() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();

    map.put(AzureComputeInstanceTemplateConfigurationProperty.STORAGE_TYPE.unwrap()
        .getConfigKey(), "Premium__LRS"); // two underscores

    validator.checkStorage(new SimpleConfiguration(map), accumulator, localizationContext);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkStorageWithValidPremiumStorageAccountSizeExpectNoError() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(STORAGE_ACCOUNT_TYPE, SkuName.PREMIUM_LRS.toString());

    final List<String> diskSizes = new ArrayList<>();
    diskSizes.add("1");
    diskSizes.add("511");
    diskSizes.add("512");
    diskSizes.add("513");
    diskSizes.add("1023");
    diskSizes.add("1024");
    diskSizes.add("1025");
    diskSizes.add("2047");
    diskSizes.add("2048");
    diskSizes.add("2049");
    diskSizes.add("4095");

    for (String diskSize : diskSizes) {
      map.put(DATA_DISK_SIZE, diskSize);

      validator.checkStorage(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(0, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkStorageWithInvalidPremiumStorageAccountSizeExpectAccumulatesErrors()
      throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(STORAGE_ACCOUNT_TYPE, SkuName.PREMIUM_LRS.toString());

    final List<String> diskSizes = new ArrayList<>();
    diskSizes.add("-1");
    diskSizes.add("0");
    diskSizes.add("4096");

    for (String diskSize : diskSizes) {
      map.put(DATA_DISK_SIZE, diskSize);

      validator.checkStorage(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(1, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkStorageWithValidStandardStorageAccountSizeExpectNoError() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(STORAGE_ACCOUNT_TYPE, SkuName.STANDARD_LRS.toString());

    final List<String> diskSizes = new ArrayList<>();
    diskSizes.add("1");
    diskSizes.add("511");
    diskSizes.add("512");
    diskSizes.add("513");
    diskSizes.add("1023");
    diskSizes.add("1024");
    diskSizes.add("1025");
    diskSizes.add("2047");
    diskSizes.add("2048");
    diskSizes.add("2049");
    diskSizes.add("4095");

    for (String diskSize : diskSizes) {
      map.put(DATA_DISK_SIZE, diskSize);

      validator.checkStorage(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(0, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkStorageWithInvalidStandardStorageAccountSizeExpectAccumulatesErrors()
      throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(STORAGE_ACCOUNT_TYPE, SkuName.STANDARD_LRS.toString());

    final List<String> diskSizes = new ArrayList<>();
    diskSizes.add("-1");
    diskSizes.add("0");
    diskSizes.add("4096");
    diskSizes.add("4097");

    for (String diskSize : diskSizes) {
      map.put(DATA_DISK_SIZE, diskSize);

      validator.checkStorage(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(1, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkStorageWithInvalidStorageAccountTypeExpectAccumulatesErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(STORAGE_ACCOUNT_TYPE, SkuName.STANDARD_RAGRS.toString());

    validator.checkStorage(new SimpleConfiguration(map), accumulator, localizationContext);
    // only 1 error because we don't validate sizes on storage account types not in the default list
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());

    accumulator.getConditionsByKey().clear();
  }

  @Test
  public void checkStorageWithNonDefaultStorageAccountTypeAnySizeExpectNoError() throws Exception {
    // set the plugin config to null so this test can custom set it
    TestHelper.setAzurePluginConfigNull();

    // build a custom config with the storage accounts to test
    final List<String> accountTypeConfig = new ArrayList<>();
    accountTypeConfig.add(SkuName.PREMIUM_LRS.toString());
    accountTypeConfig.add(SkuName.STANDARD_LRS.toString());
    accountTypeConfig.add(SkuName.STANDARD_RAGRS.toString());

    Map<String, Object> configMap = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIG_FILENAME).root().unwrapped();
    ((Map<String, Object>) configMap.get(Configurations.AZURE_CONFIG_INSTANCE))
        .put(Configurations.AZURE_CONFIG_INSTANCE_STORAGE_ACCOUNT_TYPES, accountTypeConfig);

    Config config = ConfigFactory.parseMap(configMap);
    AzurePluginConfigHelper.setAzurePluginConfig(config);

    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(STORAGE_ACCOUNT_TYPE, SkuName.STANDARD_RAGRS.toString());

    final List<String> diskSizes = new ArrayList<>();
    diskSizes.add("-1");
    diskSizes.add("0");
    diskSizes.add("1");
    diskSizes.add("511");
    diskSizes.add("512");
    diskSizes.add("513");
    diskSizes.add("1023");
    diskSizes.add("1024");
    diskSizes.add("1025");
    diskSizes.add("2047");
    diskSizes.add("2048");
    diskSizes.add("2049");
    diskSizes.add("4095");
    diskSizes.add("4096");
    diskSizes.add("4097");
    diskSizes.add("8192");

    for (String diskSize : diskSizes) {
      map.put(DATA_DISK_SIZE, diskSize);

      validator.checkStorage(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(0, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkStorageWithNonexistentStorageAccountTypeExpectAccumulatesErrors()
      throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(STORAGE_ACCOUNT_TYPE, "this_is_not_a_storage_account_type");

    validator.checkStorage(new SimpleConfiguration(map), accumulator, localizationContext);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkSshUsernameWithValidUsernameExpectNoError() throws Exception {
    validator.checkSshUsername(TestHelper.buildValidDirectorUnitTestConfig(), accumulator,
        localizationContext);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkSSHUsernameWithDisallowedUsernameExpectAccumulatesErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();

    for (String disallowedUsername : AzurePluginConfigHelper.getAzurePluginConfigInstanceSection()
        .getStringList(Configurations.AZURE_CONFIG_DISALLOWED_USERNAMES)) {
      map.put(SSH_USERNAME, disallowedUsername);

      validator.checkSshUsername(new SimpleConfiguration(map), accumulator, localizationContext);
      Assert.assertEquals(1, accumulator.getConditionsByKey().size());

      accumulator.getConditionsByKey().clear();
    }
  }

  @Test
  public void checkVmImageWithImageNotInConfigExpectAccumulatesErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(IMAGE, "fake-image");

    validator.checkVmImage(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVmImageWithExistingImageMissingFieldsExpectAccumulatesErrors() throws Exception {
    // Set the configurable images to null so this test can custom set it
    TestHelper.setConfigurableImagesNull();

    Map<String, Object> imagesMap = AzurePluginConfigHelper
        .parseConfigFromClasspath(Configurations.AZURE_CONFIGURABLE_IMAGES_FILE).root().unwrapped();
    ((Map<String, Object>) imagesMap.get(TestHelper.TEST_CENTOS_IMAGE_NAME)).clear();
    AzurePluginConfigHelper.setConfigurableImages(ConfigFactory.parseMap(imagesMap));

    validator.checkVmImage(TestHelper.buildValidDirectorUnitTestConfig(), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVmImageWithInvalidUriMissingFieldsExpectAccumulatesErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/missing/fields");

    validator.checkVmImage(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkVmImageWithValidUriInvalidFieldsExpectAccumulatesErrors() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorLiveTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/not-publisher/cloudera/not-offer/cloudera-centos-os/not-sku/7_2/not-version/latest");

    validator.checkVmImage(new SimpleConfiguration(map), accumulator, localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkUseCustomImageWithCustomImageDisabledExpectNoError() throws Exception {
    validator.checkUseCustomImage(TestHelper.buildValidDirectorUnitTestConfig(), accumulator,
        localizationContext, azure);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkUseCustomImageWithCorrectConfigsExpectNoError() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE.unwrap()
        .getConfigKey(), "Yes");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/some/invalid/custom/image/id");

    azure = Mockito.mock(Azure.class);
    VirtualMachineCustomImages images = Mockito.mock(VirtualMachineCustomImages.class);
    VirtualMachineCustomImage image = Mockito.mock(VirtualMachineCustomImage.class);
    Mockito.when(image.region()).thenReturn(Region.fromName(TestHelper.TEST_REGION));
    Mockito.when(images.getById(ArgumentMatchers.anyString())).thenReturn(image);
    Mockito.when(azure.virtualMachineCustomImages()).thenReturn(images);

    validator.checkUseCustomImage(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(0, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkUseCustomImageWithNoManagedDiskExpectError() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE.unwrap()
        .getConfigKey(), "Yes");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.MANAGED_DISKS.unwrap()
        .getConfigKey(), "No");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/some/invalid/custom/image/id");

    validator.checkUseCustomImage(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkUseCustomImageWithImageDosNotExistExpectError() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE.unwrap()
        .getConfigKey(), "Yes");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/some/invalid/custom/image/id");

    azure = Mockito.mock(Azure.class);
    VirtualMachineCustomImages images = Mockito.mock(VirtualMachineCustomImages.class);
    Mockito.when(images.getById(ArgumentMatchers.anyString())).thenReturn(null);
    Mockito.when(azure.virtualMachineCustomImages()).thenReturn(images);

    validator.checkUseCustomImage(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkUseCustomImageWithImageInDifferentRegionExpectError() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE.unwrap()
        .getConfigKey(), "Yes");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/some/invalid/custom/image/id");

    azure = Mockito.mock(Azure.class);
    VirtualMachineCustomImages images = Mockito.mock(VirtualMachineCustomImages.class);
    VirtualMachineCustomImage image = Mockito.mock(VirtualMachineCustomImage.class);
    // return a region that is different from the default test one
    Mockito.when(image.region()).thenReturn(Region.GERMANY_CENTRAL);
    Mockito.when(images.getById(ArgumentMatchers.anyString())).thenReturn(image);
    Mockito.when(azure.virtualMachineCustomImages()).thenReturn(images);

    validator.checkUseCustomImage(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  @Test
  public void checkUseCustomImageWithInvalidPlanExpectError() throws Exception {
    Map<String, String> map = TestHelper.buildValidDirectorUnitTestMap();
    map.put(AzureComputeInstanceTemplateConfigurationProperty.USE_CUSTOM_MANAGED_IMAGE.unwrap()
        .getConfigKey(), "Yes");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.IMAGE.unwrap().getConfigKey(),
        "/some/invalid/custom/image/id");
    map.put(AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_IMAGE_PLAN.unwrap()
        .getConfigKey(), "foobar");

    azure = Mockito.mock(Azure.class);
    VirtualMachineCustomImages images = Mockito.mock(VirtualMachineCustomImages.class);
    VirtualMachineCustomImage image = Mockito.mock(VirtualMachineCustomImage.class);
    Mockito.when(image.region()).thenReturn(Region.fromName(TestHelper.TEST_REGION));
    Mockito.when(images.getById(ArgumentMatchers.anyString())).thenReturn(image);
    Mockito.when(azure.virtualMachineCustomImages()).thenReturn(images);

    validator.checkUseCustomImage(new SimpleConfiguration(map), accumulator,
        localizationContext, azure);
    Assert.assertEquals(1, accumulator.getConditionsByKey().size());
  }

  /**
   * Test to verify the following valid custom image purchase plan configs are allowed:
   * - null and empty string
   * - valid plan with that follows the format: /publisher/<value>/product/<value>/name/<value>
   * - valid plan with valid format (above case) with extra trailing slashes ("/")
   *
   * @throws Exception
   */
  @Test
  public void parseCustomImagePurchasePlanFromConfigTestExpectNoError() throws Exception {
    Map<String, String> map = new HashMap<>();

    // no config
    Assert.assertNull(Configurations.parseCustomImagePurchasePlanFromConfig(
        new SimpleConfiguration(map), localizationContext));

    map.put(AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_IMAGE_PLAN.unwrap()
        .getConfigKey(), "");
    Assert.assertNull(Configurations.parseCustomImagePurchasePlanFromConfig(
        new SimpleConfiguration(map), localizationContext));

    map.put(AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_IMAGE_PLAN.unwrap()
        .getConfigKey(), "/publisher/fooPublisher/product/fooProduct/name/fooName");
    PurchasePlan plan = Configurations.parseCustomImagePurchasePlanFromConfig(
        new SimpleConfiguration(map), localizationContext);
    Assert.assertNotNull(plan);
    Assert.assertTrue(plan.name().equals("fooName"));
    Assert.assertTrue(plan.publisher().equals("fooPublisher"));
    Assert.assertTrue(plan.product().equals("fooProduct"));

    // trailing slash is OK
    map.put(AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_IMAGE_PLAN.unwrap()
        .getConfigKey(), "/publisher/fooPublisher/product/fooProduct/name/fooName/////////");
    PurchasePlan plan2 = Configurations.parseCustomImagePurchasePlanFromConfig(
        new SimpleConfiguration(map), localizationContext);
    Assert.assertNotNull(plan2);
    Assert.assertTrue(plan2.name().equals("fooName"));
    Assert.assertTrue(plan2.publisher().equals("fooPublisher"));
    Assert.assertTrue(plan2.product().equals("fooProduct"));
  }

  @Test
  public void parseCustomImageWithInvalidPurchasePlanExpectError() throws Exception {
    Map<String, String> map = new HashMap<>();

    List<String> invalidPlans = new ArrayList<>();
    invalidPlans.add("/invalid/plan/too/short");
    invalidPlans.add("/invalid/plan/that/is/too/too/long");
    // leading slash is not OK
    invalidPlans.add("/////////publisher/foo/product/foo/name/foo");
    // extra slash in the middle is not OK
    invalidPlans.add("/publisher/foo/////product/foo/name/foo");
    // wrong key
    invalidPlans.add("/wrong/foo/key/foo/used/foo");

    for (String invalidPlan : invalidPlans) {
      boolean caughtException = false;
      map.put(AzureComputeInstanceTemplateConfigurationProperty.CUSTOM_IMAGE_PLAN.unwrap()
          .getConfigKey(), invalidPlan);
      try {
        Configurations.parseCustomImagePurchasePlanFromConfig(new SimpleConfiguration(map),
            localizationContext);
      } catch (ValidationException e) {
        LOG.info("Expected exception: ", e);
        caughtException = true;
      }
      Assert.assertTrue(caughtException);
    }
  }
}
