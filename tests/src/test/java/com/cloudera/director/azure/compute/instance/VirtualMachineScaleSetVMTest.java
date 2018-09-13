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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.cloudera.director.azure.shaded.com.google.common.collect.Sets;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;

import org.junit.Test;

public class VirtualMachineScaleSetVMTest {

  @Test
  public void testProxiedMethodAllImplemented() throws InvocationTargetException, IllegalAccessException {
    com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineScaleSetVM vm = mock(
        com.cloudera.director.azure.shaded.com.microsoft.azure.management.compute.VirtualMachineScaleSetVM.class,
        invocation -> null);

    VirtualMachineScaleSetVM obj = VirtualMachineScaleSetVM.create(vm, null);
    Method[] methods = AzureInstance.class.getMethods();
    Set<String> methodNames = Sets.newHashSet(VirtualMachineScaleSetVM.METHODS.keySet());

    for (Method method : methods) {
      methodNames.remove(method.getName());
      assertThat(method.invoke(obj)).isNull();
    }

    assertThat(methodNames.isEmpty()).isTrue();
  }
}
