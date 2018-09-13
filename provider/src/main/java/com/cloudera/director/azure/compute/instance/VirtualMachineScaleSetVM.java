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

import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.management.network.NetworkInterfaceBase;
import com.microsoft.azure.management.network.implementation.PublicIPAddressInner;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Interface for {@linkplain com.microsoft.azure.management.compute.VirtualMachineScaleSetVM} to work with {@linkplain
 * AzureInstance}.
 */
public interface VirtualMachineScaleSetVM
    extends com.microsoft.azure.management.compute.VirtualMachineScaleSetVM, AzureInstance {
  // Azure SDK impl: goo.gl/7GgozZ
  String PRIMARY_NETWORK_INTERFACE_NAME = "primary-nic-cfg";

  // Note: please keep the method name same as the ones in AzureInstance, backed by utest
  Map<String, BiFunction<com.microsoft.azure.management.compute.VirtualMachineScaleSetVM, PublicIPAddressInner, Object>> METHODS =
      ImmutableMap.of(
      "getPrimaryNetworkInterface", VirtualMachineScaleSetVM::getPrimaryNetworkInterface,
      "getPublicIPAddress", VirtualMachineScaleSetVM::getPublicIPAddress);

  /**
   * Creates proxy for {@linkplain com.microsoft.azure.management.compute.VirtualMachineScaleSetVM} to respond to all
   * {@linkplain AzureInstance} method calls.
   *
   * @param vm the virtual machine object to be proxied
   * @return a proxied virtual machine object
   */
  static VirtualMachineScaleSetVM create(
      com.microsoft.azure.management.compute.VirtualMachineScaleSetVM vm,
      PublicIPAddressInner publicIPAddressInner) {
    return vm == null
        ? null
        : (VirtualMachineScaleSetVM) Proxy
        .newProxyInstance(
            VirtualMachineScaleSetVM.class.getClassLoader(),
            new Class[]{VirtualMachineScaleSetVM.class},
            (proxy, method, args) ->
                METHODS.containsKey(method.getName())
                    ? METHODS.get(method.getName()).apply(vm, publicIPAddressInner)
                    : method.invoke(vm, args));
  }

  /**
   * Adaptor method to get network interface.
   *
   * @param vm virtual machine
   * @return network interface
   */
  static NetworkInterfaceBase getPrimaryNetworkInterface(
      com.microsoft.azure.management.compute.VirtualMachineScaleSetVM vm,
      PublicIPAddressInner publicIPAddressInner) {
    return vm == null ? null : vm.getNetworkInterface(PRIMARY_NETWORK_INTERFACE_NAME);
  }

  /**
   * Adaptor method to get public IP address
   *
   * @param vm virtual machine
   * @return public IP address
   */
  static PublicIPAddressInner getPublicIPAddress(
      com.microsoft.azure.management.compute.VirtualMachineScaleSetVM vm,
      PublicIPAddressInner publicIPAddressInner) {
    return publicIPAddressInner;
  }
}
