/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord.http;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.indexer.TaskInfo;
import io.druid.indexer.TaskLocation;
import io.druid.indexer.TaskStatus;
import io.druid.indexer.TaskStatusPlus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.AbstractTask;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.IndexerMetadataStorageAdapter;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.TaskStorageQueryAdapter;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.Resource;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.List;

public class OverlordResourceTest
{
  private OverlordResource overlordResource;
  private TaskMaster taskMaster;
  private TaskStorageQueryAdapter taskStorageQueryAdapter;
  private IndexerMetadataStorageAdapter indexerMetadataStorageAdapter;
  private HttpServletRequest req;
  private TaskRunner taskRunner;

  @Before
  public void setUp()
  {
    taskRunner = EasyMock.createMock(TaskRunner.class);
    taskMaster = EasyMock.createStrictMock(TaskMaster.class);
    taskStorageQueryAdapter = EasyMock.createStrictMock(TaskStorageQueryAdapter.class);
    indexerMetadataStorageAdapter = EasyMock.createStrictMock(IndexerMetadataStorageAdapter.class);
    req = EasyMock.createStrictMock(HttpServletRequest.class);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(
        Optional.of(taskRunner)
    ).anyTimes();

    AuthorizerMapper authMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return new Authorizer()
        {
          @Override
          public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
          {
            if (resource.getName().equals("allow")) {
              return new Access(true);
            } else {
              return new Access(false);
            }
          }

        };
      }
    };

    overlordResource = new OverlordResource(
        taskMaster,
        taskStorageQueryAdapter,
        null,
        null,
        null,
        null,
        authMapper,
        null,
        null,
        null
    );
  }

  @Test
  public void testSecuredGetWaitingTask()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(taskStorageQueryAdapter.getActiveTaskInfo(null)).andStubReturn(
        ImmutableList.<TaskInfo<Task, TaskStatus>>of(
            new TaskInfo(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "allow",
                getTaskWithIdAndDatasource("id_1", "allow")
            ),
            new TaskInfo(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_2"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            ),
            new TaskInfo(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_3"),
                "deny",
                getTaskWithIdAndDatasource("id_3", "deny")
            ),
            new TaskInfo(
                "id_4",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_4"),
                "deny",
                getTaskWithIdAndDatasource("id_4", "deny")
            )
        )
    );

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1", null),
            new MockTaskRunnerWorkItem("id_4", null)
        )
    );

    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);

    List<TaskStatusPlus> responseObjects = (List) overlordResource.getWaitingTasks(req)
                                                                                       .getEntity();
    Assert.assertEquals(1, responseObjects.size());
    Assert.assertEquals("id_2", responseObjects.get(0).getId());
  }

  @Test
  public void testSecuredGetCompleteTasks()
  {
    expectAuthorizationTokenCheck();
    List<String> tasksIds = ImmutableList.of("id_1", "id_2", "id_3");
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getRunningTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem(tasksIds.get(0), null),
            new MockTaskRunnerWorkItem(tasksIds.get(1), null),
            new MockTaskRunnerWorkItem(tasksIds.get(2), null)));

    EasyMock.expect(taskStorageQueryAdapter.getRecentlyCompletedTaskInfo(null, null, null)).andStubReturn(
        ImmutableList.<TaskInfo<Task, TaskStatus>>of(
            new TaskInfo(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "deny",
                getTaskWithIdAndDatasource("id_1", "deny")
            ),
            new TaskInfo(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_2"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            ),
            new TaskInfo(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_3"),
                "allow",
                getTaskWithIdAndDatasource("id_3", "allow")
            )
        )
    );
    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);
    Assert.assertTrue(taskStorageQueryAdapter.getRecentlyCompletedTaskInfo(null, null, null).size() == 3);
    Assert.assertTrue(taskRunner.getRunningTasks().size() == 3);
    List<TaskStatusPlus> responseObjects = (List) overlordResource
          .getCompleteTasks(null, req).getEntity();

    Assert.assertEquals(2, responseObjects.size());
    Assert.assertEquals(tasksIds.get(1), responseObjects.get(0).getId());
    Assert.assertEquals(tasksIds.get(2), responseObjects.get(1).getId());
  }

  @Test
  public void testSecuredGetRunningTasks()
  {
    expectAuthorizationTokenCheck();
    List<String> tasksIds = ImmutableList.of("id_1", "id_2");
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getRunningTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem(tasksIds.get(0), null),
            new MockTaskRunnerWorkItem(tasksIds.get(1), null)
        )
    );
    EasyMock.expect(taskStorageQueryAdapter.getActiveTaskInfo(null)).andStubReturn(
        ImmutableList.<TaskInfo<Task, TaskStatus>>of(
            new TaskInfo(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "deny",
                getTaskWithIdAndDatasource("id_1", "deny")
            ),
            new TaskInfo(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_2"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            )
        )
    );

    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);

    List<TaskStatusPlus> responseObjects = (List) overlordResource.getRunningTasks(null,req)
                                                                                       .getEntity();

    Assert.assertEquals(1, responseObjects.size());
    Assert.assertEquals(tasksIds.get(1), responseObjects.get(0).getId());
  }

  @Test
  public void testGetTasks()
  {
    expectAuthorizationTokenCheck();
    //completed tasks
    EasyMock.expect(taskStorageQueryAdapter.getRecentlyCompletedTaskInfo(null, null, null)).andStubReturn(
        ImmutableList.<TaskInfo<Task, TaskStatus>>of(
            new TaskInfo(
                "id_5",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_5"),
                "deny",
                getTaskWithIdAndDatasource("id_5", "deny")
            ),
            new TaskInfo(
                "id_6",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_6"),
                "allow",
                getTaskWithIdAndDatasource("id_6", "allow")
            ),
            new TaskInfo(
                "id_7",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_7"),
                "allow",
                getTaskWithIdAndDatasource("id_7", "allow")
            )
        )
    );
    //active tasks
    EasyMock.expect(taskStorageQueryAdapter.getActiveTaskInfo(null)).andStubReturn(
        ImmutableList.<TaskInfo<Task, TaskStatus>>of(
            new TaskInfo(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "allow",
                getTaskWithIdAndDatasource("id_1", "allow")
            ),
            new TaskInfo(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_2"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            ),
            new TaskInfo(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_3"),
                "deny",
                getTaskWithIdAndDatasource("id_3", "deny")
            ),
            new TaskInfo(
                "id_4",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_4"),
                "deny",
                getTaskWithIdAndDatasource("id_4", "deny")
            )
        )
    );

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1", null),
            new MockTaskRunnerWorkItem("id_4", null)
        )
    ).atLeastOnce();

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getPendingTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_4", null)
        )
    );

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getRunningTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1", null)
        )
    );

    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);
    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks(null, null, null, null, null, req, null)
        .getEntity();
    Assert.assertEquals(4, responseObjects.size());
  }

  @Test
  public void testGetTasksFilterDataSource()
  {
    expectAuthorizationTokenCheck();
    //completed tasks
    EasyMock.expect(taskStorageQueryAdapter.getRecentlyCompletedTaskInfo(null, null, "allow")).andStubReturn(
        ImmutableList.<TaskInfo<Task, TaskStatus>>of(
            new TaskInfo(
                "id_5",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_5"),
                "allow",
                getTaskWithIdAndDatasource("id_5", "allow")
            ),
            new TaskInfo(
                "id_6",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_6"),
                "allow",
                getTaskWithIdAndDatasource("id_6", "allow")
            ),
            new TaskInfo(
                "id_7",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_7"),
                "allow",
                getTaskWithIdAndDatasource("id_7", "allow")
            )
        )
    );
    //active tasks
    EasyMock.expect(taskStorageQueryAdapter.getActiveTaskInfo("allow")).andStubReturn(
        ImmutableList.<TaskInfo<Task, TaskStatus>>of(
            new TaskInfo(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "allow",
                getTaskWithIdAndDatasource("id_1", "allow")
            ),
            new TaskInfo(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            ),
            new TaskInfo(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "allow",
                getTaskWithIdAndDatasource("id_3", "allow")
            ),
            new TaskInfo(
                "id_4",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_4"),
                "allow",
                getTaskWithIdAndDatasource("id_4", "allow")
            )
        )
    );
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1", null),
            new MockTaskRunnerWorkItem("id_4", null)
        )
    ).atLeastOnce();
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getPendingTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_4", null)
        )
    );

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getRunningTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1", null)
        )
    );
    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);

    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks(null, "allow", null, null, null, req, null)
        .getEntity();
    Assert.assertEquals(7, responseObjects.size());
    Assert.assertEquals("id_5", responseObjects.get(0).getId());
    Assert.assertTrue("DataSource Check", "allow".equals(responseObjects.get(0).getDataSource()));
  }

  @Test
  public void testGetTasksFilterWaitingState()
  {
    expectAuthorizationTokenCheck();
    //active tasks
    EasyMock.expect(taskStorageQueryAdapter.getActiveTaskInfo(null)).andStubReturn(
        ImmutableList.<TaskInfo<Task, TaskStatus>>of(
            new TaskInfo(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "allow",
                getTaskWithIdAndDatasource("id_1", "allow")
            ),
            new TaskInfo(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_2"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            ),
            new TaskInfo(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_3"),
                "deny",
                getTaskWithIdAndDatasource("id_3", "deny")
            ),
            new TaskInfo(
                "id_4",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_4"),
                "deny",
                getTaskWithIdAndDatasource("id_4", "deny")
            )
        )
    );

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1", null),
            new MockTaskRunnerWorkItem("id_4", null)
        )
    );

    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);
    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks(
            "waiting",
            null,
            null,
            null,
            null,
            req,
            null
        ).getEntity();
    Assert.assertEquals(1, responseObjects.size());
    Assert.assertEquals("id_2", responseObjects.get(0).getId());
  }

  @Test
  public void testGetTasksFilterRunningState()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(taskStorageQueryAdapter.getActiveTaskInfo("allow")).andStubReturn(
        ImmutableList.<TaskInfo<Task, TaskStatus>>of(
            new TaskInfo(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "allow",
                getTaskWithIdAndDatasource("id_1", "allow")
            ),
            new TaskInfo(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_2"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            ),
            new TaskInfo(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_3"),
                "allow",
                getTaskWithIdAndDatasource("id_3", "allow")
            ),
            new TaskInfo(
                "id_4",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_4"),
                "deny",
                getTaskWithIdAndDatasource("id_4", "deny")
            )
        )
    );

    List<String> tasksIds = ImmutableList.of("id_1", "id_2");
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getRunningTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem(tasksIds.get(0), null),
            new MockTaskRunnerWorkItem(tasksIds.get(1), null)
        )
    );


    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);

    List<TaskStatusPlus> responseObjects = (List) overlordResource
        .getTasks("running", "allow", null, null, null, req, null)
        .getEntity();

    Assert.assertEquals(2, responseObjects.size());
    Assert.assertEquals(tasksIds.get(0), responseObjects.get(0).getId());
    String ds = responseObjects.get(0).getDataSource();
    Assert.assertTrue("DataSource Check", "allow".equals(responseObjects.get(0).getDataSource()));
  }

  @Test
  public void testGetTasksFilterPendingState()
  {
    expectAuthorizationTokenCheck();
    List<String> tasksIds = ImmutableList.of("id_1", "id_2");
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getPendingTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem(tasksIds.get(0), null),
            new MockTaskRunnerWorkItem(tasksIds.get(1), null)
        )
    );
    EasyMock.expect(taskStorageQueryAdapter.getActiveTaskInfo(null)).andStubReturn(
        ImmutableList.<TaskInfo<Task, TaskStatus>>of(
            new TaskInfo(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "deny",
                getTaskWithIdAndDatasource("id_1", "deny")
            ),
            new TaskInfo(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_2"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            ),
            new TaskInfo(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_3"),
                "allow",
                getTaskWithIdAndDatasource("id_3", "allow")
            ),
            new TaskInfo(
                "id_4",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_4"),
                "deny",
                getTaskWithIdAndDatasource("id_4", "deny")
            )
        )
    );


    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);

    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks("pending", null, null, null, null, req, null)
        .getEntity();

    Assert.assertEquals(1, responseObjects.size());
    Assert.assertEquals(tasksIds.get(1), responseObjects.get(0).getId());
    String ds = responseObjects.get(0).getDataSource();
    //Assert.assertTrue("DataSource Check", "ds_test".equals(responseObjects.get(0).getDataSource()));
  }

  @Test
  public void testGetTasksFilterCompleteState()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(taskStorageQueryAdapter.getRecentlyCompletedTaskInfo(null, null, null)).andStubReturn(
        ImmutableList.<TaskInfo<Task, TaskStatus>>of(
            new TaskInfo(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "allow",
                getTaskWithIdAndDatasource("id_1", "allow")
            ),
            new TaskInfo(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_2"),
                "deny",
                getTaskWithIdAndDatasource("id_2", "deny")
            ),
            new TaskInfo(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_3"),
                "allow",
                getTaskWithIdAndDatasource("id_3", "allow")
            )
        )
    );
    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);
    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks("complete", null, null, null, null, req, null)
        .getEntity();
    Assert.assertEquals(2, responseObjects.size());
    Assert.assertEquals("id_1", responseObjects.get(0).getId());
    Assert.assertTrue("DataSource Check", "allow".equals(responseObjects.get(0).getDataSource()));
  }

  @Test
  public void testGetTasksFilterCompleteStateWithInterval()
  {
    expectAuthorizationTokenCheck();
    List<String> tasksIds = ImmutableList.of("id_1", "id_2", "id_3");
    Duration duration = new Period("PT86400S").toStandardDuration();
    EasyMock.expect(taskStorageQueryAdapter.getRecentlyCompletedTaskInfo(null, duration, null)).andStubReturn(
        ImmutableList.<TaskInfo<Task, TaskStatus>>of(
            new TaskInfo(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "deny",
                getTaskWithIdAndDatasource("id_1", "deny")
            ),
            new TaskInfo(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_2"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            ),
            new TaskInfo(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_3"),
                "allow",
                getTaskWithIdAndDatasource("id_3", "allow")
            )
        )
    );

    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);
    String period = "P1D";
    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks("complete", null, period, null, null, req, null)
        .getEntity();
    Assert.assertEquals(2, responseObjects.size());
    Assert.assertEquals("id_2", responseObjects.get(0).getId());
    Assert.assertTrue("DataSource Check", "allow".equals(responseObjects.get(0).getDataSource()));
  }

  @Test
  public void testGetTasksNegativeState()
  {
    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);
    Object responseObject = overlordResource
        .getTasks("blah", "ds_test", null, null, null, req, null)
        .getEntity();
    Assert.assertEquals(
        "Invalid state : blah, valid values are: [pending, waiting, running, complete]",
        responseObject.toString()
    );
  }

  @Test
  public void testSecuredTaskPost()
  {
    expectAuthorizationTokenCheck();
    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);
    Task task = NoopTask.create();
    Response response = overlordResource.taskPost(task, req);
    Assert.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);
  }

  private Task getTaskWithIdAndDatasource(String id, String datasource)
  {
    return new AbstractTask(id, datasource, null)
    {
      @Override
      public String getType()
      {
        return null;
      }

      @Override
      public boolean isReady(TaskActionClient taskActionClient) throws Exception
      {
        return false;
      }

      @Override
      public TaskStatus run(TaskToolbox toolbox) throws Exception
      {
        return null;
      }
    };
  }

  private static class MockTaskRunnerWorkItem extends TaskRunnerWorkItem
  {
    public MockTaskRunnerWorkItem(
        String taskId,
        ListenableFuture<TaskStatus> result
    )
    {
      super(taskId, result);
    }

    @Override
    public TaskLocation getLocation()
    {
      return TaskLocation.unknown();
    }

    @Nullable
    @Override
    public String getTaskType()
    {
      return "test";
    }

    @Override
    public String getDataSource()
    {
      return "ds_test";
    }

  }

  private void expectAuthorizationTokenCheck()
  {
    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null, null);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(authenticationResult)
            .atLeastOnce();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().anyTimes();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
  }
}
