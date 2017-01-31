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

package com.cloudera.director.azure.compute.provider;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.cloudera.director.azure.compute.instance.TaskResult;
import com.cloudera.director.azure.utils.VmCreationParameters;
import com.microsoft.azure.management.compute.models.VirtualMachine;
import com.microsoft.azure.utility.ResourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to run the VM create and delete tasks.
 */
public class TaskRunner {
  // Visible for test only
  ExecutorService service = Executors.newCachedThreadPool();

  private static final int THREAD_POOL_SHUTDOWN_WAIT_TIME_SECONDS = 30;

  private static final Logger LOG = LoggerFactory.getLogger(TaskRunner.class);

  private TaskRunner() {}

  /**
   * Simple builder to allow mocking of TaskRunner
   *
   * @return a new TaskRunner object
   */
  public static TaskRunner build() {
    return new TaskRunner();
  }

  /**
   * Submit a VM creation task
   *
   * @param computeProviderHelper AzureComputeProviderHelper object for interacting with Azure
   *                              backend
   * @param context    Azure resource context. Stores detailed info of VM supporting resources when
   *                   the function returns
   * @param parameters contains additional parameters used for VM creation that is not in the
   *                   Azure resource context
   * @param azureOperationPollingTimeout Azure operation polling timeout specified in second
   * @return
   */
  public Future<TaskResult> submitVmCreationTask(
      AzureComputeProviderHelper computeProviderHelper, ResourceContext context,
      VmCreationParameters parameters, int azureOperationPollingTimeout) {

    // Create a task to request resources and create VM
    CreateVMTask task = new CreateVMTask(context, parameters, azureOperationPollingTimeout,
        computeProviderHelper);

    return service.submit(task);
  }

  /**
   * Submit a VM deletion task
   *
   * @param computeProviderHelper        AzureComputeProviderHelper object for interacting with
   *                                     Azure backend
   * @param resourceGroup                name of the resource group VM resides in
   * @param vm                           object that contains various information of the VM
   * @param isPublicIPConfigured         is the resource provisioned with a public IP
   * @param azureOperationPollingTimeout Azure operation polling timeout specified in second
   * @return
   */
  public Future<TaskResult> submitDeleteVmTask(AzureComputeProviderHelper computeProviderHelper,
      String resourceGroup, VirtualMachine vm,
      boolean isPublicIPConfigured, int azureOperationPollingTimeout) {
    CleanUpTask toDelete = new CleanUpTask(resourceGroup, vm, computeProviderHelper,
        isPublicIPConfigured, azureOperationPollingTimeout);
    return service.submit(toDelete);
  }

  /**
   * Submit and run tasks to delete VM resources
   *
   * Blocks until all resources specified in contexts are deletes or if deletion thread is
   * interrupted.
   *
   * @param computeProviderHelper
   * @param resourceGroup                Azure resource group name
   * @param contexts                     Azure context populated during VM allocation
   * @param isPublicIPConfigured         was the resource provisioned with a public IP
   * @param azureOperationPollingTimeout Azure operation polling timeout specified in second
   * @throws InterruptedException
   */
  public void submitAndRunResourceDeleteTasks(AzureComputeProviderHelper computeProviderHelper,
      String resourceGroup, Collection<ResourceContext> contexts, boolean isPublicIPConfigured,
      int azureOperationPollingTimeout) throws InterruptedException {
    Set<CleanUpTask> tasks = new HashSet<>();
    for (ResourceContext context : contexts) {
      tasks.add(new CleanUpTask(resourceGroup, context, computeProviderHelper, isPublicIPConfigured,
          azureOperationPollingTimeout));
    }
    service.invokeAll(tasks);
  }

  /**
   * Poll a single pending task till it is complete or timeout.
   * Used for test only.
   *
   * @param task             pending task to poll for completion
   * @param durationInSecond overall timeout period
   * @param intervalInSecond poll interval
   * @return number of successful task (0 or 1)
   */
  public int pollPendingTask(Future<TaskResult> task, int durationInSecond, int intervalInSecond) {
    Set<Future<TaskResult>> operations = new HashSet<>();
    operations.add(task);
    return pollPendingTasks(operations, durationInSecond, intervalInSecond, null);
  }

  /**
   * Poll pending tasks till all tasks are complete or timeout.
   * Azure platform operation can range from minutes to one hour.
   *
   * @param tasks            set of submitted tasks
   * @param durationInSecond overall timeout period
   * @param intervalInSecond poll interval
   * @param failedContexts   set of failed task contexts. This list contains all the contexts of
   *                         submitted tasks. Context of a successful task is removed from this
   *                         set. When this call returns the element in this set are the contexts
   *                         of failed tasks.
   * @return number of successful tasks
   */
  @SuppressWarnings("PMD.CollapsibleIfStatements")
  public int pollPendingTasks(Set<Future<TaskResult>> tasks, int durationInSecond,
      int intervalInSecond, Set<ResourceContext> failedContexts) {
    Set<Future<TaskResult>> responses = new HashSet<>(tasks);
    int succeededCount = 0;
    int timerInMilliSec = durationInSecond * 1000;
    int intervalInMilliSec = intervalInSecond * 1000;

    try {
      while (timerInMilliSec > 0 && responses.size() > 0) {
        Set<Future<TaskResult>> dones = new HashSet<>();
        for (Future<TaskResult> task : responses) {
          try {
            if (task.isDone()) {
              dones.add(task);
              TaskResult tr = task.get();
              if (tr.isSuccessful()) {
                succeededCount++;
                // Remove successful contexts so that what remains are the failed contexts
                if (failedContexts != null) {
                  if (!failedContexts.remove(tr.getContex())) {
                    LOG.error("ResourceContext {} does not exist in the submitted context list.",
                        tr.getContex());
                  }
                }
              }
            }
          } catch (ExecutionException e) {
            LOG.error("Polling of pending tasks encountered an error: ", e);
          }
        }
        responses.removeAll(dones);

        Thread.sleep(intervalInMilliSec);

        timerInMilliSec = timerInMilliSec - intervalInMilliSec;
        LOG.debug("Polling pending tasks: remaining time = " + timerInMilliSec / 1000 +
            " seconds.");
      }
    } catch (InterruptedException e) {
      LOG.error("Polling of pending tasks was interrupted.", e);
      shutdownTaskRunnerService();
    }

    // Terminate all tasks if we timed out.
    if (timerInMilliSec <= 0 && responses.size() > 0) {
      shutdownTaskRunnerService();
    }

    // Always return the succeeded task count and let the caller decide if any resources needs to be
    // cleaned up
    return succeededCount;
  }

  private void shutdownTaskRunnerService() {
    LOG.debug("Shutting down task runner service.");
    service.shutdownNow();
    try {
      boolean terminated = service.awaitTermination(THREAD_POOL_SHUTDOWN_WAIT_TIME_SECONDS,
          TimeUnit.SECONDS);
      if (terminated == false) {
        LOG.error("Thread pool shutdown timeout elapsed before all resources were terminated");
      }
    } catch (InterruptedException e) {
      LOG.error("Shutdown of thread pool was interrupted.", e);
    }
  }
}
