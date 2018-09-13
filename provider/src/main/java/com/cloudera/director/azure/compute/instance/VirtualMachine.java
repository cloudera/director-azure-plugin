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
import com.microsoft.azure.management.network.implementation.PublicIPAddressInner;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.function.Function;

/**
 * Interface for {@linkplain com.microsoft.azure.management.compute.VirtualMachine} to work with {@linkplain
 * AzureInstance}.
 */
public interface VirtualMachine extends com.microsoft.azure.management.compute.VirtualMachine, AzureInstance {

  // Note: please keep the method name same as the ones in AzureInstance, backed by utest
  Map<String, Function<com.microsoft.azure.management.compute.VirtualMachine, Object>> METHODS =
      ImmutableMap.of("getPublicIPAddress", VirtualMachine::getPublicIPAddress);

  /**
   * Creates proxy for {@linkplain com.microsoft.azure.management.compute.VirtualMachine} to respond to all
   * {@linkplain AzureInstance} method calls.
   *
   * @param vm the virtual machine object to be proxied
   * @return a proxied virtual machine object
   */
  static VirtualMachine create(com.microsoft.azure.management.compute.VirtualMachine vm) {
    return vm == null
        ? null
        : (VirtualMachine) Proxy
        .newProxyInstance(
            VirtualMachine.class.getClassLoader(),
            new Class[]{VirtualMachine.class},
            (proxy, method, args) ->
                METHODS.containsKey(method.getName())
                    ? METHODS.get(method.getName()).apply(vm)
                    : method.invoke(vm, args));
  }

  /**
   * Adaptor method to get public IP address
   *
   * @param vm virtual machine
   * @return public IP address
   */
  static PublicIPAddressInner getPublicIPAddress(com.microsoft.azure.management.compute.VirtualMachine vm) {
    return vm == null || vm.getPrimaryPublicIPAddress() == null ? null : vm.getPrimaryPublicIPAddress().inner();
  }
}
