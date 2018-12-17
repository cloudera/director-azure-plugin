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
 */

package com.cloudera.director.azure.compute.instance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cloudera.director.azure.shaded.com.microsoft.azure.management.network.NetworkInterfaceBase;

import java.net.InetAddress;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public class AzureComputeInstanceTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetPrivateIpAddress() {
    AzureInstance instance = mock(AzureInstance.class);
    NetworkInterfaceBase nic = mock(NetworkInterfaceBase.class);

    when(instance.getPrimaryNetworkInterface()).thenReturn(nic);
    when(nic.primaryPrivateIP()).thenReturn("203.0.113.123");

    InetAddress privateIpAddress = AzureComputeInstance.getPrivateIpAddress(instance);
    assertThat(privateIpAddress.getHostAddress()).isEqualTo("203.0.113.123");
  }

  @Test
  public void testGetPrivateIpAddressNullInstance() {
    assertThat(AzureComputeInstance.getPrivateIpAddress(null)).isNull();
  }

  @Test
  public void testGetPrivateIpAddressNullNic() {
    AzureInstance instance = mock(AzureInstance.class);
    when(instance.getPrimaryNetworkInterface()).thenReturn(null);

    assertThat(AzureComputeInstance.getPrivateIpAddress(instance)).isNull();
  }

  @Test
  public void testGetPrivateIpAddressNullPrivateIP() {
    AzureInstance instance = mock(AzureInstance.class);
    NetworkInterfaceBase nic = mock(NetworkInterfaceBase.class);

    when(instance.getPrimaryNetworkInterface()).thenReturn(nic);
    when(nic.primaryPrivateIP()).thenReturn(null);

    assertThat(AzureComputeInstance.getPrivateIpAddress(instance)).isNull();
  }

  @Test
  public void testGetPrivateIpAddressEmptyPrivateIP() {
    AzureInstance instance = mock(AzureInstance.class);
    NetworkInterfaceBase nic = mock(NetworkInterfaceBase.class);

    when(instance.getPrimaryNetworkInterface()).thenReturn(nic);
    when(nic.primaryPrivateIP()).thenReturn("");

    assertThat(AzureComputeInstance.getPrivateIpAddress(instance)).isNull();
  }

  @Test
  public void testGetPrivateIpAddressInvalid() {
    thrown.expect(IllegalArgumentException.class);

    AzureInstance instance = mock(AzureInstance.class);
    NetworkInterfaceBase nic = mock(NetworkInterfaceBase.class);

    when(instance.getPrimaryNetworkInterface()).thenReturn(nic);
    when(nic.primaryPrivateIP()).thenReturn("free real estate");

    AzureComputeInstance.getPrivateIpAddress(instance);
  }
}
