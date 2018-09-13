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

package com.cloudera.director.azure.compute.instance;

import com.microsoft.azure.management.compute.StorageProfile;
import com.microsoft.azure.management.compute.VirtualMachineSizeTypes;
import com.microsoft.azure.management.network.NetworkInterfaceBase;
import com.microsoft.azure.management.network.implementation.PublicIPAddressInner;

/**
 * Common interface for {@linkplain com.microsoft.azure.management.compute.VirtualMachine} and {@linkplain
 * com.microsoft.azure.management.compute.VirtualMachineScaleSetVM}.
 */
public interface AzureInstance {
  /**
   * Returns the primary network interface.
   *
   * @return the primary network interface
   */
  NetworkInterfaceBase getPrimaryNetworkInterface();

  /**
   * Returns the storage profile.
   *
   * @return the storage profile
   */
  StorageProfile storageProfile();

  /**
   * Returns the public IP address.
   *
   * @return the public IP address
   */
  PublicIPAddressInner getPublicIPAddress();

  /**
   * Returns the region name.
   *
   * @return the region name
   */
  String regionName();

  /**
   * Returns the resource name of the instance.
   *
   * @return the resource name of the instance
   */
  String name();

  /**
   * Returns the computer name of the instance.
   *
   * @return the computer name of the instance
   */
  String computerName();

  /**
   * Returns {@linkplain VirtualMachineSizeTypes}.
   *
   * @return the virtual machine size type
   */
  VirtualMachineSizeTypes size();
}
