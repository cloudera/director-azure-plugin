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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.cloudera.director.azure.compute.instance.TaskResult;
import com.cloudera.director.azure.shaded.com.microsoft.azure.utility.ResourceContext;
import org.junit.Before;
import org.junit.Test;

/**
 * Mock tests for TaskRunner class
 */
public class TaskRunnerTest {
  TaskRunner taskRunner;
  ExecutorService service;

  @Before
  public void setup() throws IllegalAccessException, InvocationTargetException,
      InstantiationException, NoSuchMethodException, NoSuchFieldException {
    service = mock(ExecutorService.class, RETURNS_DEEP_STUBS);
    taskRunner = spy(TaskRunner.build());
    taskRunner.service = service;
  }

  //
  // pollPendingTasks() Tests
  //
  @Test
  public void pollPendingTasks_validShutdownWithFailedContexts_success() throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenReturn(true);

    Future<TaskResult> ftr = mock(Future.class);
    TaskResult tr = mock(TaskResult.class);
    ResourceContext rc = mock(ResourceContext.class);

    when(ftr.isDone())
      .thenReturn(true);
    when(ftr.get())
      .thenReturn(tr);

    when(tr.isSuccessful())
      .thenReturn(true);
    when(tr.getContex())
      .thenReturn(rc);

    Set<Future<TaskResult>> tasks = new HashSet<Future<TaskResult>>();
    tasks.add(ftr);
    Set<ResourceContext> failedContexts = new HashSet<ResourceContext>();
    failedContexts.add(rc);

    int succeededCount = taskRunner.pollPendingTasks(tasks, 10, 1, failedContexts);

    assertEquals(1, succeededCount);
  }

  @Test
  public void pollPendingTasks_validShutdownWithNoFailedContexts_success() throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenReturn(true);

    Future<TaskResult> ftr = mock(Future.class);
    TaskResult tr = mock(TaskResult.class);

    when(ftr.isDone())
      .thenReturn(true);
    when(ftr.get())
      .thenReturn(tr);

    when(tr.isSuccessful())
      .thenReturn(true);

    Set<Future<TaskResult>> tasks = new HashSet<Future<TaskResult>>();
    tasks.add(ftr);
    Set<ResourceContext> failedContexts = null;

    int succeededCount = taskRunner.pollPendingTasks(tasks, 10, 1, failedContexts);

    assertEquals(1, succeededCount);
  }

  @Test
  public void pollPendingTasks_validShutdownWithNonexistentContexts_error() throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenReturn(true);

    Future<TaskResult> ftr = mock(Future.class);
    TaskResult tr = mock(TaskResult.class);

    when(ftr.isDone())
      .thenReturn(true);
    when(ftr.get())
      .thenReturn(tr);

    when(tr.isSuccessful())
      .thenReturn(true);

    Set<Future<TaskResult>> tasks = new HashSet<Future<TaskResult>>();
    tasks.add(ftr);
    Set<ResourceContext> failedContexts = new HashSet<ResourceContext>();

    int succeededCount = taskRunner.pollPendingTasks(tasks, 10, 1, failedContexts);

    assertEquals(1, succeededCount);
  }

  @Test
  public void pollPendingTasks_invalidShutdown_ExecutionException() throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenReturn(true);

    Future<TaskResult> ftr = mock(Future.class);
    when(ftr.isDone())
      .thenReturn(true);
    when(ftr.get())
      .thenThrow(new ExecutionException(null));

    Set<Future<TaskResult>> tasks = new HashSet<Future<TaskResult>>();
    tasks.add(ftr);
    Set<ResourceContext> failedContexts = new HashSet<ResourceContext>();

    int succeededCount = taskRunner.pollPendingTasks(tasks, 10, 1, failedContexts);

    assertEquals(0, succeededCount);
  }

  @Test
  public void pollPendingTasks_invalidShutdown_InterruptedException() throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenReturn(true);

    Future<TaskResult> ftr = mock(Future.class);
    when(ftr.isDone())
      .thenReturn(true);
    when(ftr.get())
      .thenThrow(new InterruptedException(null));

    Set<Future<TaskResult>> tasks = new HashSet<Future<TaskResult>>();
    tasks.add(ftr);
    Set<ResourceContext> failedContexts = new HashSet<ResourceContext>();

    int succeededCount = taskRunner.pollPendingTasks(tasks, 10, 1, failedContexts);

    assertEquals(0, succeededCount);
  }

  @Test
  public void pollPendingTasks_timeOut_terminateAllTasks() throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenReturn(true);

    Future<TaskResult> ftr = mock(Future.class);
    TaskResult tr = mock(TaskResult.class);
    ResourceContext rc = mock(ResourceContext.class);

    when(ftr.isDone())
      .thenReturn(true);
    when(ftr.get())
      .thenReturn(tr);

    when(tr.isSuccessful())
      .thenReturn(true);
    when(tr.getContex())
      .thenReturn(rc);

    Set<Future<TaskResult>> tasks = new HashSet<Future<TaskResult>>();
    tasks.add(ftr);
    Set<ResourceContext> failedContexts = new HashSet<ResourceContext>();
    failedContexts.add(rc);

    int succeededCount = taskRunner.pollPendingTasks(tasks, 0, 1, failedContexts);

    assertEquals(0, succeededCount);
  }


  //
  // shutdownTaskRunnerService() Tests
  //

  @Test
  public void shutdownTaskRunnerService_elapsedTimeoutShutdown_error() throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenReturn(false);

    Class clazz = TaskRunner.class;
    Method method = clazz.getDeclaredMethod("shutdownTaskRunnerService");
    method.setAccessible(true);

    method.invoke(taskRunner);

    verify(service).awaitTermination(anyLong(), any(TimeUnit.class));
  }

  @Test
  public void shutdownTaskRunnerService_interruptedShutdown_InterruptedExceptionCaught()
    throws Exception {
    when(service.shutdownNow())
      .thenReturn(mock(List.class));
    when(service.awaitTermination(anyLong(), any(TimeUnit.class)))
      .thenThrow(new InterruptedException());

    Class clazz = TaskRunner.class;
    Method method = clazz.getDeclaredMethod("shutdownTaskRunnerService");
    method.setAccessible(true);

    method.invoke(taskRunner);

    verify(service).awaitTermination(anyLong(), any(TimeUnit.class));
  }
}
