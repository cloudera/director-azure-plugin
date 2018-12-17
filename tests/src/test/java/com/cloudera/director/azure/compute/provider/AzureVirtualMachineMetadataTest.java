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
 *  limitations under the License.
 *
 */

package com.cloudera.director.azure.compute.provider;

import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getBase64EncodedCustomData;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getFirstGroupOfUuid;
import static com.cloudera.director.azure.compute.provider.AzureVirtualMachineMetadata.getVmId;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.BaseEncoding;

import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

public class AzureVirtualMachineMetadataTest {
  @Test
  public void testGetFirstGroupOfUuidIsNullSafe() {
    assertThat(getFirstGroupOfUuid(null)).isNull();
  }

  @Test
  public void testGetCustomDataWithEncodedCustomData() {
    String customData = "hello world";
    String encodedCustomData = BaseEncoding.base64().encode(customData.getBytes(StandardCharsets.UTF_8));
    String actual = getBase64EncodedCustomData(null, encodedCustomData);
    assertThat(actual).isEqualTo(encodedCustomData);
    actual = getBase64EncodedCustomData(StringUtils.EMPTY, encodedCustomData);
    assertThat(actual).isEqualTo(encodedCustomData);
  }

  @Test
  public void testGetCustomDataWithUnencodedCustomData() {
    String customData = "hello world";
    String encodedCustomData = BaseEncoding.base64().encode(customData.getBytes(StandardCharsets.UTF_8));
    String actual = getBase64EncodedCustomData(customData, null);
    assertThat(actual).isEqualTo(encodedCustomData);
    actual = getBase64EncodedCustomData(customData, StringUtils.EMPTY);
    assertThat(actual).isEqualTo(encodedCustomData);
  }

  @Test
  public void testGetNullCustomData() {
    String actual = getBase64EncodedCustomData(null, null);
    assertThat(actual).isNull();
    actual = getBase64EncodedCustomData(StringUtils.EMPTY, null);
    assertThat(actual).isNull();
    actual = getBase64EncodedCustomData(null, StringUtils.EMPTY);
    assertThat(actual).isNull();
    actual = getBase64EncodedCustomData(StringUtils.EMPTY, StringUtils.EMPTY);
    assertThat(actual).isNull();
  }

  @Test
  public void testGetInstanceId() {
    assertThat(getVmId("prefix-testId", "prefix")).isEqualTo("testId");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetInstanceIdBadPrefix() {
    getVmId("prefix-testId", "badPrefix-");
  }

  @Test
  public void getHostKeysFromCommandOutput() {
    String output = "[some-nonsense]\n" +
        "MD5:aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99\n" +
        "MD5:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99:aa\n" +
        "\n" +
        "[some-more-nonsense]";
    assertThat(AzureVirtualMachineMetadata.getHostKeysFromCommandOutput(output)).containsOnly(
        "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99",
        "bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99:aa");
  }
}
