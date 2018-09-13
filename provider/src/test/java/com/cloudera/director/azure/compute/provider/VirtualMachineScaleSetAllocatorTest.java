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

import static com.cloudera.director.azure.compute.instance.VirtualMachineScaleSetVM.create;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cloudera.director.azure.compute.instance.AzureComputeInstance;
import com.cloudera.director.azure.compute.instance.AzureComputeInstanceTemplate;
import com.cloudera.director.azure.compute.provider.VirtualMachineScaleSetAllocator.VirtualMachineStateComparator;
import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.management.compute.PowerState;
import com.microsoft.azure.management.compute.VirtualMachineScaleSetVM;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

public class VirtualMachineScaleSetAllocatorTest {

  private final VirtualMachineStateComparator comparator = new VirtualMachineStateComparator();
  private final Map<Pair<PowerState, PowerState>, Equality> comparisons = ImmutableMap
      .<Pair<PowerState, PowerState>, Equality>builder()
      .put(Pair.of(PowerState.RUNNING, PowerState.RUNNING), Equality.EQ)
      .put(Pair.of(PowerState.RUNNING, PowerState.STARTING), Equality.LT)
      .put(Pair.of(PowerState.RUNNING, PowerState.DEALLOCATED), Equality.LT)
      .put(Pair.of(PowerState.RUNNING, PowerState.DEALLOCATING), Equality.LT)
      .put(Pair.of(PowerState.RUNNING, PowerState.STOPPING), Equality.LT)
      .put(Pair.of(PowerState.RUNNING, PowerState.STOPPED), Equality.LT)
      .put(Pair.of(PowerState.RUNNING, PowerState.UNKNOWN), Equality.LT)
      .put(Pair.of(PowerState.STARTING, PowerState.RUNNING), Equality.GT)
      .put(Pair.of(PowerState.STARTING, PowerState.STARTING), Equality.EQ)
      .put(Pair.of(PowerState.STARTING, PowerState.UNKNOWN), Equality.LT)
      .put(Pair.of(PowerState.STARTING, PowerState.STOPPED), Equality.LT)
      .put(Pair.of(PowerState.STARTING, PowerState.STOPPING), Equality.LT)
      .put(Pair.of(PowerState.STARTING, PowerState.DEALLOCATING), Equality.LT)
      .put(Pair.of(PowerState.STARTING, PowerState.DEALLOCATED), Equality.LT)
      .build();

  @Test
  public void testGetInstanceIdFromPublicIpId() {
    String publicIpId = "/subscriptions/38ff619b-c7ac-42d7-b914-f5ec88910d07"
        + "/resourceGroups/director-master-livetest"
        + "/providers/Microsoft.Compute"
        + "/virtualMachineScaleSets/d1d2ede9"
        + "/virtualMachines/6"
        + "/networkInterfaces/primary-nic-cfg"
        + "/ipConfigurations/primary-nic-ip-cfg"
        + "/publicIPAddresses/d1d2ede9";

    assertThat(VirtualMachineScaleSetAllocator.getInstanceId(publicIpId)).isEqualTo("6");
  }

  @Test
  public void testVirtualMachineStateComparator() {
    VirtualMachineScaleSetVM lhs = mock(VirtualMachineScaleSetVM.class);
    VirtualMachineScaleSetVM rhs = mock(VirtualMachineScaleSetVM.class);
    AzureComputeInstanceTemplate template = mock(AzureComputeInstanceTemplate.class);

    for (Entry<Pair<PowerState, PowerState>, Equality> comparison : comparisons.entrySet()) {
      when(lhs.powerState()).thenReturn(comparison.getKey().getLeft());
      when(rhs.powerState()).thenReturn(comparison.getKey().getRight());
      assertThat(comparison
          .getValue()
          .test(comparator.compare(
              new AzureComputeInstance(template, "lhs", create(lhs, null)),
              new AzureComputeInstance(template, "rhs", create(rhs, null)))))
          .isTrue();
    }
  }

  @Test
  public void testVirtualMachineStateComparatorInSyncWithAzureSDK() {
    PowerState
        .values()
        .stream()
        .forEach(ps -> assertThat(VirtualMachineStateComparator.POWER_STATES.get(ps)).isNotNull());
  }

  private enum Equality implements Predicate<Integer> {
    LT(i -> i < 0),
    EQ(i -> i == 0),
    GT(i -> i > 0);

    private final Predicate<Integer> predicate;

    Equality(Predicate<Integer> predicate) {
      this.predicate = predicate;
    }

    @Override
    public boolean test(Integer integer) {
      return predicate.test(integer);
    }
  }
}
