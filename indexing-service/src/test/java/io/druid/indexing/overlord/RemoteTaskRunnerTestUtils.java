/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.http.client.HttpClient;
import io.druid.common.guava.DSuppliers;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.curator.cache.PathChildrenCacheFactory;
import io.druid.curator.cache.SimplePathChildrenCacheFactory;
import io.druid.indexing.common.IndexingServiceCondition;
import io.druid.indexer.TaskLocation;
import io.druid.indexer.TaskStatus;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.autoscaling.NoopResourceManagementStrategy;
import io.druid.indexing.overlord.autoscaling.ResourceManagementStrategy;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.indexing.worker.TaskAnnouncement;
import io.druid.indexing.worker.Worker;
import io.druid.server.initialization.IndexerZkConfig;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.zookeeper.CreateMode;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class RemoteTaskRunnerTestUtils
{
  static final Joiner joiner = Joiner.on("/");
  static final String basePath = "/test/druid";
  static final String announcementsPath = String.format("%s/indexer/announcements", basePath);
  static final String tasksPath = String.format("%s/indexer/tasks", basePath);
  static final String statusPath = String.format("%s/indexer/status", basePath);
  static final TaskLocation DUMMY_LOCATION = TaskLocation.create("dummy", 9000);

  private TestingCluster testingCluster;

  private CuratorFramework cf;
  private ObjectMapper jsonMapper;

  RemoteTaskRunnerTestUtils()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
  }

  CuratorFramework getCuratorFramework()
  {
    return cf;
  }

  ObjectMapper getObjectMapper()
  {
    return jsonMapper;
  }

  void setUp() throws Exception
  {
    testingCluster = new TestingCluster(1);
    testingCluster.start();

    cf = CuratorFrameworkFactory.builder()
                                .connectString(testingCluster.getConnectString())
                                .retryPolicy(new ExponentialBackoffRetry(1, 10))
                                .compressionProvider(new PotentiallyGzippedCompressionProvider(false))
                                .build();
    cf.start();
    cf.blockUntilConnected();
    cf.create().creatingParentsIfNeeded().forPath(basePath);
    cf.create().creatingParentsIfNeeded().forPath(tasksPath);
  }

  void tearDown() throws Exception
  {
    cf.close();
    testingCluster.stop();
  }

  RemoteTaskRunner makeRemoteTaskRunner(RemoteTaskRunnerConfig config) throws Exception
  {
    RemoteTaskRunner remoteTaskRunner = new TestableRemoteTaskRunner(
        jsonMapper,
        config,
        new IndexerZkConfig(
            new ZkPathsConfig()
            {
              @Override
              public String getBase()
              {
                return basePath;
              }
            }, null, null, null, null, null
        ),
        cf,
        new SimplePathChildrenCacheFactory.Builder().build(),
        null,
        DSuppliers.of(new AtomicReference<>(WorkerBehaviorConfig.defaultConfig())),
        ScheduledExecutors.fixed(1, "Remote-Task-Runner-Cleanup--%d"),
        new NoopResourceManagementStrategy<WorkerTaskRunner>()
    );

    remoteTaskRunner.start();
    return remoteTaskRunner;
  }

  Worker makeWorker(final String workerId, final int capacity) throws Exception
  {
    Worker worker = new Worker(
        workerId,
        workerId,
        capacity,
        "0"
    );

    cf.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(
        joiner.join(announcementsPath, workerId),
        jsonMapper.writeValueAsBytes(worker)
    );
    cf.create().creatingParentsIfNeeded().forPath(joiner.join(tasksPath, workerId));

    return worker;
  }

  void disableWorker(Worker worker) throws Exception
  {
    cf.setData().forPath(
        joiner.join(announcementsPath, worker.getHost()),
        jsonMapper.writeValueAsBytes(new Worker(worker.getHost(), worker.getIp(), worker.getCapacity(), ""))
    );
  }

  void mockWorkerRunningTask(final String workerId, final Task task) throws Exception
  {
    cf.delete().forPath(joiner.join(tasksPath, workerId, task.getId()));

    TaskAnnouncement taskAnnouncement = TaskAnnouncement.create(task, TaskStatus.running(task.getId()), DUMMY_LOCATION);
    cf.create()
      .creatingParentsIfNeeded()
      .forPath(joiner.join(statusPath, workerId, task.getId()), jsonMapper.writeValueAsBytes(taskAnnouncement));
  }

  void mockWorkerCompleteSuccessfulTask(final String workerId, final Task task) throws Exception
  {
    TaskAnnouncement taskAnnouncement = TaskAnnouncement.create(task, TaskStatus.success(task.getId()), DUMMY_LOCATION);
    cf.setData().forPath(joiner.join(statusPath, workerId, task.getId()), jsonMapper.writeValueAsBytes(taskAnnouncement));
  }

  void mockWorkerCompleteFailedTask(final String workerId, final Task task) throws Exception
  {
    TaskAnnouncement taskAnnouncement = TaskAnnouncement.create(task, TaskStatus.failure(task.getId(), "reason"), DUMMY_LOCATION);
    cf.setData().forPath(joiner.join(statusPath, workerId, task.getId()), jsonMapper.writeValueAsBytes(taskAnnouncement));
  }

  boolean workerRunningTask(final String workerId, final String taskId)
  {
    return pathExists(joiner.join(statusPath, workerId, taskId));
  }

  boolean taskAnnounced(final String workerId, final String taskId)
  {
    return pathExists(joiner.join(tasksPath, workerId, taskId));
  }

  boolean pathExists(final String path)
  {
    return TestUtils.conditionValid(
        new IndexingServiceCondition()
        {
          @Override
          public boolean isValid()
          {
            try {
              return cf.checkExists().forPath(path) != null;
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );
  }

  public static class TestableRemoteTaskRunner extends RemoteTaskRunner
  {
    private long currentTimeMillis = System.currentTimeMillis();

    public TestableRemoteTaskRunner(
        ObjectMapper jsonMapper,
        RemoteTaskRunnerConfig config,
        IndexerZkConfig indexerZkConfig,
        CuratorFramework cf,
        PathChildrenCacheFactory pathChildrenCacheFactory,
        HttpClient httpClient,
        Supplier<WorkerBehaviorConfig> workerConfigRef,
        ScheduledExecutorService cleanupExec,
        ResourceManagementStrategy<WorkerTaskRunner> resourceManagement
    )
    {
      super(
          jsonMapper,
          config,
          indexerZkConfig,
          cf,
          pathChildrenCacheFactory,
          httpClient,
          workerConfigRef,
          cleanupExec,
          resourceManagement
      );
    }

    void setCurrentTimeMillis(long currentTimeMillis)
    {
      this.currentTimeMillis = currentTimeMillis;
    }

    @Override
    protected long getCurrentTimeMillis()
    {
      return currentTimeMillis;
    }
  }
}
