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

package com.cloudera.director.azure.compute.provider;

import static com.microsoft.azure.management.compute.models.ComputeOperationStatus.InProgress;
import static com.microsoft.azure.management.compute.models.ComputeOperationStatus.Succeeded;

import com.microsoft.azure.management.compute.models.ComputeLongRunningOperationResponse;
import com.microsoft.azure.management.compute.models.ComputeOperationResponse;
import com.microsoft.azure.management.compute.models.ComputeOperationStatus;
import com.microsoft.windowsazure.exception.ServiceException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;

/**
 * Abstract class that provides a polling mechanism for specific Create/Delete tasks.
 */
public abstract class AbstractAzureComputeProviderTask {
  protected AzureComputeProviderHelper computeProviderHelper = null;
  protected int defaultSleepIntervalInSec = 10; //10 second
  protected int defaultTimeoutInSec = 1200; //20 minutes

  protected int pollPendingOperations(Set<ComputeOperationResponse> vmOperations, int durationInSecond, int
    intervalinSecond, Logger log, String operationTarget) throws InterruptedException {
    Set<ComputeOperationResponse> responses = new HashSet<ComputeOperationResponse>(vmOperations);
    int succeededCount = 0;
    int timerInMilliSec = durationInSecond * 1000;
    int intervalInMilliSec = intervalinSecond * 1000;
    while (timerInMilliSec > 0 && responses.size() > 0) {
      Set<ComputeOperationResponse> dones = new HashSet<ComputeOperationResponse>();
      for (ComputeOperationResponse response : responses) {
        try {
          ComputeLongRunningOperationResponse lroResponse = computeProviderHelper.getLongRunningOperationStatus(
            response.getAzureAsyncOperation());
          ComputeOperationStatus status = lroResponse.getStatus();
          log.debug("Operation (id = {}) status is {}.", response.getRequestId(), status);
          if (!status.equals(InProgress)) {
            dones.add(response);
          }
          if (status.equals(Succeeded)) {
            succeededCount++;
            //additional cleanup for delete operation go here?
          }
        } catch (IOException | ServiceException e) {
          // FIXME Extend TaskResult class to record errors occured during create/delete tasks.
          log.error("Encountered error while polling for Azure long running operations: ", e);
          dones.add(response);
        }
      }

      responses.removeAll(dones);
      Thread.sleep(intervalInMilliSec);
      timerInMilliSec = timerInMilliSec - intervalInMilliSec;
      log.debug("{} Polling pending operations: remaining time = {} seconds.", operationTarget,
        timerInMilliSec / 1000 );
    }

    log.debug("{} Done polling pending operations.", operationTarget);
    return succeededCount;
  }

  protected int pollPendingOperation(ComputeOperationResponse vmOperation, int durationInSecond,
    int intervalinSecond, Logger log, String operationTarget)
    throws InterruptedException {
    Set<ComputeOperationResponse> operations = new HashSet();
    operations.add(vmOperation);
    return pollPendingOperations(operations, durationInSecond, intervalinSecond, log,
      operationTarget);
  }
}
