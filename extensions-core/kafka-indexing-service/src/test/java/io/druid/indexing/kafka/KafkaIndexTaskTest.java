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

package io.druid.indexing.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.MapCache;
import io.druid.common.utils.StringUtils;
import io.druid.concurrent.Execs;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.JSONPathFieldSpec;
import io.druid.data.input.impl.JSONPathSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.Granularities;
import io.druid.granularity.QueryGranularities;
import io.druid.indexer.TaskState;
import io.druid.indexer.TaskStatus;
import io.druid.indexing.common.SegmentLoaderFactory;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.LocalTaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.kafka.supervisor.KafkaSupervisor;
import io.druid.indexing.kafka.test.TestBroker;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.MetadataTaskStorage;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.indexing.test.TestDataSegmentAnnouncer;
import io.druid.indexing.test.TestDataSegmentKiller;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.core.LoggingEmitter;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.metrics.MonitorScheduler;
import io.druid.metadata.DerbyMetadataStorageActionHandlerFactory;
import io.druid.metadata.EntryExistsException;
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import io.druid.metadata.TestDerbyConnector;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.Druids;
import io.druid.query.NoopQueryWatcher;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPusherConfig;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoaderLocalCacheManager;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.segment.realtime.appenderator.AppenderatorImpl;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.server.coordination.DataSegmentServerAnnouncer;
import io.druid.timeline.DataSegment;
import org.apache.curator.test.TestingCluster;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RunWith(Parameterized.class)
public class KafkaIndexTaskTest
{
  private static final Logger log = new Logger(KafkaIndexTaskTest.class);
  private static final ObjectMapper objectMapper = new DefaultObjectMapper();
  private static final long POLL_RETRY_MS = 100;

  private static TestingCluster zkServer;
  private static TestBroker kafkaServer;
  private static ServiceEmitter emitter;
  private static ListeningExecutorService taskExec;
  private static int topicPostfix;

  private final List<Task> runningTasks = Lists.newArrayList();
  private final boolean buildV9Directly= true;

  private long handoffConditionTimeout = 0;
  private boolean reportParseExceptions = false;
  private boolean resetOffsetAutomatically = false;
  private boolean doHandoff = true;
  private Integer maxRowsPerSegment = null;

  private TaskToolboxFactory toolboxFactory;
  private IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private TaskStorage taskStorage;
  private TaskLockbox taskLockbox;
  private File directory;
  private String topic;
  private List<ProducerRecord<byte[], byte[]>> records;
  private final boolean isIncrementalHandoffSupported;
  private final Set<Integer> checkpointRequestsHash = Sets.newHashSet();

  // This should be removed in versions greater that 0.11.1
  // isIncrementalHandoffSupported should always be set to true in those later versions
  @Parameterized.Parameters(name = "isIncrementalHandoffSupported = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{true}, new Object[]{false});
  }

  public KafkaIndexTaskTest(boolean isIncrementalHandoffSupported)
  {
    this.isIncrementalHandoffSupported = isIncrementalHandoffSupported;
  }

  private static final DataSchema DATA_SCHEMA = new DataSchema(
      "test_ds",
      objectMapper.convertValue(
          new StringInputRowParser(
              new JSONParseSpec(
                  new DefaultTimestampSpec("timestamp", "iso", null),
                  new DimensionsSpec(
                      DimensionsSpec.getDefaultSchemas(ImmutableList.<String>of("dim1", "dim2")),
                      null,
                      null
                  ),
                  new JSONPathSpec(true, ImmutableList.<JSONPathFieldSpec>of()),
                  ImmutableMap.<String, Boolean>of()
              ),
              Charsets.UTF_8.name()
          ),
          Map.class
      ),
      new AggregatorFactory[]{new CountAggregatorFactory("rows")},
      new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
      objectMapper
  );

  private static List<ProducerRecord<byte[], byte[]>> generateRecords(String topic)
  {
    return ImmutableList.of(
        new ProducerRecord<byte[], byte[]>(topic, 0, null, JB("2008", "a", "y", 1.0f)),
        new ProducerRecord<byte[], byte[]>(topic, 0, null, JB("2009", "b", "y", 1.0f)),
        new ProducerRecord<byte[], byte[]>(topic, 0, null, JB("2010", "c", "y", 1.0f)),
        new ProducerRecord<byte[], byte[]>(topic, 0, null, JB("2011", "d", "y", 1.0f)),
        new ProducerRecord<byte[], byte[]>(topic, 0, null, JB("2011", "e", "y", 1.0f)),
        new ProducerRecord<byte[], byte[]>(topic, 0, null, JB("246140482-04-24T15:36:27.903Z", "x", "z", 1.0f)),
        new ProducerRecord<byte[], byte[]>(topic, 0, null, "unparseable".getBytes()),
        new ProducerRecord<byte[], byte[]>(topic, 0, null, null),
        new ProducerRecord<byte[], byte[]>(topic, 0, null, JB("2013", "f", "y", 1.0f)),
        new ProducerRecord<byte[], byte[]>(topic, 1, null, JB("2012", "g", "y", 1.0f)),
        new ProducerRecord<byte[], byte[]>(topic, 1, null, JB("2011", "h", "y", 1.0f))
    );
  }

  private static String getTopicName()
  {
    return "topic" + topicPostfix++;
  }

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derby = new TestDerbyConnector.DerbyConnectorRule();

  @BeforeClass
  public static void setupClass() throws Exception
  {
    emitter = new ServiceEmitter(
        "service",
        "host",
        new LoggingEmitter(
            log,
            LoggingEmitter.Level.ERROR,
            new DefaultObjectMapper()
        )
    );
    emitter.start();
    EmittingLogger.registerEmitter(emitter);

    zkServer = new TestingCluster(1);
    zkServer.start();

    kafkaServer = new TestBroker(
        zkServer.getConnectString(),
        null,
        1,
        ImmutableMap.of("num.partitions", "2")
    );
    kafkaServer.start();

    taskExec = MoreExecutors.listeningDecorator(
        Executors.newCachedThreadPool(
            Execs.makeThreadFactory("kafka-task-test-%d")
        )
    );
  }

  @Before
  public void setupTest() throws IOException
  {
    handoffConditionTimeout = 0;
    reportParseExceptions = false;
    doHandoff = true;
    topic = getTopicName();
    records = generateRecords(topic);
    makeToolboxFactory();
  }

  @After
  public void tearDownTest()
  {
    synchronized (runningTasks) {
      for (Task task : runningTasks) {
        task.stopGracefully();
      }

      runningTasks.clear();
    }

    destroyToolboxFactory();
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    taskExec.shutdown();
    taskExec.awaitTermination(9999, TimeUnit.DAYS);

    kafkaServer.close();
    kafkaServer = null;

    zkServer.stop();
    zkServer = null;

    emitter.close();
  }

  @Test(timeout = 60_000L)
  public void testRunAfterDataInserted() throws Exception
  {
    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunBeforeDataInserted() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getStatus() != KafkaIndexTask.Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
  }

  @Test(timeout = 60_000L)
  public void testIncrementalHandOff() throws Exception
  {
    if (!isIncrementalHandoffSupported) {
      return;
    }
    final String baseSequenceName = "sequence0";
    // as soon as any segment has more than one record, incremental publishing should happen
    maxRowsPerSegment = 1;

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }
    Map<String, String> consumerProps = kafkaServer.consumerProperties();
    consumerProps.put("max.poll.records", "1");

    final KafkaPartitions startPartitions = new KafkaPartitions(topic, ImmutableMap.of(0, 0L, 1, 0L));
    // Checkpointing will happen at either checkpoint1 or checkpoint2 depending on ordering
    // of events fetched across two partitions from Kafka
    final KafkaPartitions checkpoint1 = new KafkaPartitions(topic, ImmutableMap.of(0, 5L, 1, 0L));
    final KafkaPartitions checkpoint2 = new KafkaPartitions(topic, ImmutableMap.of(0, 4L, 1, 2L));
    final KafkaPartitions endPartitions = new KafkaPartitions(topic, ImmutableMap.of(0, 9L, 1, 2L));
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            baseSequenceName,
            startPartitions,
            endPartitions,
            consumerProps,
            true,
            false,
            null,
            null,
            false
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);
    while (task.getStatus() != KafkaIndexTask.Status.PAUSED) {
      Thread.sleep(10);
    }
    final Map<Integer, Long> currentOffsets = ImmutableMap.copyOf(task.getCurrentOffsets());
    Assert.assertTrue(checkpoint1.getPartitionOffsetMap().equals(currentOffsets) || checkpoint2.getPartitionOffsetMap()
                                                                                               .equals(currentOffsets));
    task.setEndOffsets(currentOffsets, true, false);
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    Assert.assertEquals(1, checkpointRequestsHash.size());
    Assert.assertTrue(checkpointRequestsHash.contains(
        Objects.hash(
            DATA_SCHEMA.getDataSource(),
            baseSequenceName,
            new KafkaDataSourceMetadata(startPartitions),
            new KafkaDataSourceMetadata(new KafkaPartitions(topic, currentOffsets))
        )
    ));

    // Check metrics
    Assert.assertEquals(8, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(2, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(1, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2008/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2009/P1D", 0);
    SegmentDescriptor desc3 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc4 = SD(task, "2011/P1D", 0);
    SegmentDescriptor desc5 = SD(task, "2011/P1D", 1);
    SegmentDescriptor desc6 = SD(task, "2012/P1D", 0);
    SegmentDescriptor desc7 = SD(task, "2013/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3, desc4, desc5, desc6, desc7), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 9L, 1, 2L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("a"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("b"), readSegmentColumn("dim1", desc2));
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc3));
    Assert.assertTrue((ImmutableList.of("d", "e").equals(readSegmentColumn("dim1", desc4))
                       && ImmutableList.of("h").equals(readSegmentColumn("dim1", desc5))) ||
                      (ImmutableList.of("d", "h").equals(readSegmentColumn("dim1", desc4))
                       && ImmutableList.of("e").equals(readSegmentColumn("dim1", desc5))));
    Assert.assertEquals(ImmutableList.of("g"), readSegmentColumn("dim1", desc6));
    Assert.assertEquals(ImmutableList.of("f"), readSegmentColumn("dim1", desc7));
  }

  @Test(timeout = 60_000L)
  public void testRunWithMinimumMessageTime() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 0L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            new DateTime("2010"),
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getStatus() != KafkaIndexTask.Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(2, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunWithMaximumMessageTime() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 0L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            new DateTime("2010"),
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getStatus() != KafkaIndexTask.Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(2, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2008/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2009/P1D", 0);
    SegmentDescriptor desc3 = SD(task, "2010/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("a"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("b"), readSegmentDim1(desc2));
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc3));
  }

  @Test(timeout = 60_000L)
  public void testRunOnNothing() throws Exception
  {
    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(0, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    Assert.assertEquals(ImmutableSet.of(), publishedDescriptors());
  }

  @Test(timeout = 60_000L)
  public void testHandoffConditionTimeoutWhenHandoffOccurs() throws Exception
  {
    handoffConditionTimeout = 5_000;

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
  }

  @Test(timeout = 60_000L)
  public void testHandoffConditionTimeoutWhenHandoffDoesNotOccur() throws Exception
  {
    doHandoff = false;
    handoffConditionTimeout = 100;

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(
        isIncrementalHandoffSupported ? TaskState.SUCCESS : TaskState.FAILED,
        future.get().getStatusCode()
    );

    // Check metrics
    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
  }

  @Test(timeout = 60_000L)
  public void testReportParseExceptions() throws Exception
  {
    reportParseExceptions = true;

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 7L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.FAILED, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    Assert.assertEquals(ImmutableSet.of(), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));
  }

  @Test(timeout = 60_000L)
  public void testRunReplicas() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );
    final KafkaIndexTask task2 = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task1.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
    Assert.assertEquals(3, task2.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().thrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunConflicting() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );
    final KafkaIndexTask task2 = createTask(
        null,
        new KafkaIOConfig(
            "sequence1",
            new KafkaPartitions(topic, ImmutableMap.of(0, 3L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 9L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskState.FAILED, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task1.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
    Assert.assertEquals(3, task2.getFireDepartmentMetrics().processed());
    Assert.assertEquals(2, task2.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(1, task2.getFireDepartmentMetrics().thrownAway());

    // Check published segments & metadata, should all be from the first task
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunConflictingWithoutTransactions() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            false,
            false,
            null,
            null,
            false
        )
    );
    final KafkaIndexTask task2 = createTask(
        null,
        new KafkaIOConfig(
            "sequence1",
            new KafkaPartitions(topic, ImmutableMap.of(0, 3L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 9L)),
            kafkaServer.consumerProperties(),
            false,
            false,
            null,
            null,
            false
        )
    );

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task1.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
    Assert.assertEquals(3, task2.getFireDepartmentMetrics().processed());
    Assert.assertEquals(2, task2.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(1, task2.getFireDepartmentMetrics().thrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc3 = SD(task2, "2011/P1D", 1);
    SegmentDescriptor desc4 = SD(task2, "2013/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc3));
    Assert.assertEquals(ImmutableList.of("f"), readSegmentDim1(desc4));
  }

  @Test(timeout = 60_000L)
  public void testRunOneTaskTwoPartitions() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L, 1, 0L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L, 1, 2L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
      kafkaProducer.flush();
    }

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(5, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    // desc3 will not be created in KafkaIndexTask (0.11.1) as it does not create per Kafka partition Druid segments
    SegmentDescriptor desc3 = SD(task, "2011/P1D", 1);
    SegmentDescriptor desc4 = SD(task, "2012/P1D", 0);
    Assert.assertEquals(isIncrementalHandoffSupported
                        ? ImmutableSet.of(desc1, desc2, desc4)
                        : ImmutableSet.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 5L, 1, 2L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("g"), readSegmentDim1(desc4));

    // Check desc2/desc3 without strong ordering because two partitions are interleaved nondeterministically
    Assert.assertEquals(
        isIncrementalHandoffSupported
        ? ImmutableSet.of(ImmutableList.of("d", "e", "h"))
        : ImmutableSet.of(ImmutableList.of("d", "e"), ImmutableList.of("h")),
        isIncrementalHandoffSupported
        ? ImmutableSet.of(readSegmentColumn("dim1", desc2))
        : ImmutableSet.of(readSegmentColumn("dim1", desc2), readSegmentColumn("dim1", desc3))
    );
  }

  @Test(timeout = 60_000L)
  public void testRunTwoTasksTwoPartitions() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );
    final KafkaIndexTask task2 = createTask(
        null,
        new KafkaIOConfig(
            "sequence1",
            new KafkaPartitions(topic, ImmutableMap.of(1, 0L)),
            new KafkaPartitions(topic, ImmutableMap.of(1, 1L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task1.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
    Assert.assertEquals(1, task2.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().thrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    SegmentDescriptor desc3 = SD(task2, "2012/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 5L, 1, 1L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
    Assert.assertEquals(ImmutableList.of("g"), readSegmentDim1(desc3));
  }

  @Test(timeout = 60_000L)
  public void testRestore() throws Exception
  {
    final KafkaIndexTask task1 = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);

    // Insert some data, but not enough for the task to finish
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : Iterables.limit(records, 4)) {
        kafkaProducer.send(record).get();
      }
    }

    while (countEvents(task1) != 2) {
      Thread.sleep(25);
    }

    Assert.assertEquals(2, countEvents(task1));

    // Stop without publishing segment
    task1.stopGracefully();
    unlockAppenderatorBasePersistDirForTask(task1);

    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Start a new task
    final KafkaIndexTask task2 = createTask(
        task1.getId(),
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert remaining data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : Iterables.skip(records, 4)) {
        kafkaProducer.send(record).get();
      }
    }

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(2, task1.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
    Assert.assertEquals(1, task2.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().thrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunWithPauseAndResume() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Insert some data, but not enough for the task to finish
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : Iterables.limit(records, 4)) {
        kafkaProducer.send(record).get();
      }
      kafkaProducer.flush();
    }

    while (countEvents(task) != 2) {
      Thread.sleep(25);
    }

    Assert.assertEquals(2, countEvents(task));
    Assert.assertEquals(KafkaIndexTask.Status.READING, task.getStatus());

    Map<Integer, Long> currentOffsets = objectMapper.readValue(
        task.pause(0).getEntity().toString(),
        new TypeReference<Map<Integer, Long>>()
        {
        }
    );
    Assert.assertEquals(KafkaIndexTask.Status.PAUSED, task.getStatus());

    // Insert remaining data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : Iterables.skip(records, 4)) {
        kafkaProducer.send(record).get();
      }
    }

    try {
      future.get(10, TimeUnit.SECONDS);
      Assert.fail("Task completed when it should have been paused");
    }
    catch (TimeoutException e) {
      // carry on..
    }

    Assert.assertEquals(currentOffsets, task.getCurrentOffsets());

    task.resume();

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    Assert.assertEquals(task.getEndOffsets(), task.getCurrentOffsets());

    // Check metrics
    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunAndPauseAfterReadWithModifiedEndOffsets() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 1L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 3L)),
            kafkaServer.consumerProperties(),
            true,
            true,
            null,
            null,
            false
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    while (task.getStatus() != KafkaIndexTask.Status.PAUSED) {
      Thread.sleep(25);
    }

    // reached the end of the assigned offsets and paused instead of publishing
    Assert.assertEquals(task.getEndOffsets(), task.getCurrentOffsets());
    Assert.assertEquals(KafkaIndexTask.Status.PAUSED, task.getStatus());

    Assert.assertEquals(ImmutableMap.of(0, 3L), task.getEndOffsets());
    Map<Integer, Long> newEndOffsets = ImmutableMap.of(0, 4L);
    task.setEndOffsets(newEndOffsets, false, true);
    Assert.assertEquals(newEndOffsets, task.getEndOffsets());
    Assert.assertEquals(KafkaIndexTask.Status.PAUSED, task.getStatus());
    task.resume();

    while (task.getStatus() != KafkaIndexTask.Status.PAUSED) {
      Thread.sleep(25);
    }

    // reached the end of the updated offsets and paused
    Assert.assertEquals(newEndOffsets, task.getCurrentOffsets());
    Assert.assertEquals(KafkaIndexTask.Status.PAUSED, task.getStatus());

    // try again but with resume flag == true
    newEndOffsets = ImmutableMap.of(0, 7L);
    task.setEndOffsets(newEndOffsets, true, true);
    Assert.assertEquals(newEndOffsets, task.getEndOffsets());
    Assert.assertNotEquals(KafkaIndexTask.Status.PAUSED, task.getStatus());

    while (task.getStatus() != KafkaIndexTask.Status.PAUSED) {
      Thread.sleep(25);
    }

    Assert.assertEquals(newEndOffsets, task.getCurrentOffsets());
    Assert.assertEquals(KafkaIndexTask.Status.PAUSED, task.getStatus());

    task.resume();

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(4, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(1, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(1, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2009/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc3 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 7L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("b"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc2));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc3));
  }

  @Test(timeout = 30_000L)
  public void testRunWithOffsetOutOfRangeExceptionAndPause() throws Exception
  {
    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 2L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    runTask(task);

    while (!task.getStatus().equals(KafkaIndexTask.Status.READING)) {
      Thread.sleep(2000);
    }

    task.pause(0);

    while (!task.getStatus().equals(KafkaIndexTask.Status.PAUSED)) {
      Thread.sleep(25);
    }
  }

  @Test(timeout = 30_000L)
  public void testRunWithOffsetOutOfRangeExceptionAndNextOffsetGreaterThanLeastAvailable() throws Exception
  {
    resetOffsetAutomatically = true;
    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            new KafkaPartitions(topic, ImmutableMap.of(0, 200L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 500L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        )
    );

    runTask(task);

    while (!task.getStatus().equals(KafkaIndexTask.Status.READING)) {
      Thread.sleep(20);
    }

    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(task.getStatus(), KafkaIndexTask.Status.READING);
      // Offset should not be reset
      Assert.assertTrue(task.getCurrentOffsets().get(0) == 200L);
    }
  }

  @Test(timeout = 60_000L)
  public void testRunContextSequenceAheadOfStartingOffsets() throws Exception
  {
    // This tests the case when a replacement task is created in place of a failed test
    // which has done some incremental handoffs, thus the context will contain starting
    // sequence offsets from which the task should start reading and ignore the start offsets
    if (!isIncrementalHandoffSupported) {
      return;
    }
    // Insert data
    try (final KafkaProducer<byte[], byte[]> kafkaProducer = kafkaServer.newProducer()) {
      for (ProducerRecord<byte[], byte[]> record : records) {
        kafkaProducer.send(record).get();
      }
    }

    final TreeMap<Integer, Map<Integer, Long>> sequences = new TreeMap<>();
    // Here the sequence number is 1 meaning that one incremental handoff was done by the failed task
    // and this task should start reading from offset 2 for partition 0
    sequences.put(1, ImmutableMap.of(0, 2L));
    final Map<String, Object> context = new HashMap<>();
    context.put("checkpoints", objectMapper.writerWithType(new TypeReference<TreeMap<Integer, Map<Integer, Long>>>()
    {
    }).writeValueAsString(sequences));

    final KafkaIndexTask task = createTask(
        null,
        new KafkaIOConfig(
            "sequence0",
            // task should ignore these and use sequence info sent in the context
            new KafkaPartitions(topic, ImmutableMap.of(0, 0L)),
            new KafkaPartitions(topic, ImmutableMap.of(0, 5L)),
            kafkaServer.consumerProperties(),
            true,
            false,
            null,
            null,
            false
        ),
        context
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new KafkaDataSourceMetadata(new KafkaPartitions(topic, ImmutableMap.of(0, 5L))),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentColumn("dim1", desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentColumn("dim1", desc2));
  }

  private ListenableFuture<TaskStatus> runTask(final Task task)
  {
    try {
      taskStorage.insert(task, TaskStatus.running(task.getId()));
    }
    catch (EntryExistsException e) {
      // suppress
    }
    taskLockbox.syncFromStorage();
    final TaskToolbox toolbox = toolboxFactory.build(task);
    synchronized (runningTasks) {
      runningTasks.add(task);
    }
    return taskExec.submit(
        new Callable<TaskStatus>()
        {
          @Override
          public TaskStatus call() throws Exception
          {
            try {
              if (task.isReady(toolbox.getTaskActionClient())) {
                return task.run(toolbox);
              } else {
                throw new ISE("Task is not ready");
              }
            }
            catch (Exception e) {
              log.warn(e, "Task failed");
              return TaskStatus.failure(task.getId(), e);
            }
          }
        }
    );
  }

  private TaskLock getLock(final Task task, final Interval interval)
  {
    return Iterables.find(
        taskLockbox.findLocksForTask(task),
        new Predicate<TaskLock>()
        {
          @Override
          public boolean apply(TaskLock lock)
          {
            return lock.getInterval().contains(interval);
          }
        }
    );
  }

  private KafkaIndexTask createTask(
      final String taskId,
      final KafkaIOConfig ioConfig
  )
  {
    return createTask(taskId, DATA_SCHEMA, ioConfig);
  }

  private KafkaIndexTask createTask(
      final String taskId,
      final KafkaIOConfig ioConfig,
      final Map<String, Object> context
  )
  {
    return createTask(taskId, DATA_SCHEMA, ioConfig, context);
  }

  private KafkaIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final KafkaIOConfig ioConfig
  )
  {
    final KafkaTuningConfig tuningConfig = new KafkaTuningConfig(
        1000,
        null,
        maxRowsPerSegment,
        new Period("P1Y"),
        null,
        null,
        null,
        buildV9Directly,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically
    );
    final Map<String, Object> context = isIncrementalHandoffSupported
                                        ? ImmutableMap.of(KafkaSupervisor.IS_INCREMENTAL_HANDOFF_SUPPORTED, true)
                                        : null;
    final KafkaIndexTask task = new KafkaIndexTask(
        taskId,
        null,
        dataSchema,
        tuningConfig,
        ioConfig,
        context,
        null
    );
    task.setPollRetryMs(POLL_RETRY_MS);
    return task;
  }

  private KafkaIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final KafkaIOConfig ioConfig,
      final Map<String, Object> context
  )
  {
    final KafkaTuningConfig tuningConfig = new KafkaTuningConfig(
        1000,
        null,
        maxRowsPerSegment,
        new Period("P1Y"),
        null,
        null,
        null,
        buildV9Directly,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically
    );

    if (isIncrementalHandoffSupported) {
      context.put(KafkaSupervisor.IS_INCREMENTAL_HANDOFF_SUPPORTED, true);
    }

    final KafkaIndexTask task = new KafkaIndexTask(
        taskId,
        null,
        cloneDataSchema(dataSchema),
        tuningConfig,
        ioConfig,
        context,
        null
    );
    task.setPollRetryMs(POLL_RETRY_MS);
    return task;
  }

  private static DataSchema cloneDataSchema(final DataSchema dataSchema)
  {
    return new DataSchema(
        dataSchema.getDataSource(),
        dataSchema.getParserMap(),
        dataSchema.getAggregators(),
        dataSchema.getGranularitySpec(),
        objectMapper
    );
  }

  private QueryRunnerFactoryConglomerate makeTimeseriesOnlyConglomerate()
  {
    return new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>of(
            TimeseriesQuery.class,
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(),
                new TimeseriesQueryEngine(),
                new QueryConfig(),
                NoopQueryWatcher.instance()
            )
        )
    );
  }

  private void makeToolboxFactory() throws IOException
  {
    directory = tempFolder.newFolder();
    final TestUtils testUtils = new TestUtils();
    final ObjectMapper objectMapper = testUtils.getTestObjectMapper();
    for (Module module : new KafkaIndexTaskModule().getJacksonModules()) {
      objectMapper.registerModule(module);
    }
    final TaskConfig taskConfig = new TaskConfig(
        new File(directory, "taskBaseDir").getPath(),
        null,
        null,
        50000,
        null,
        null,
        false,
        null,
        null
    );
    final TestDerbyConnector derbyConnector = derby.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createPendingSegmentsTable();
    derbyConnector.createSegmentTable();
    derbyConnector.createRulesTable();
    derbyConnector.createConfigTable();
    derbyConnector.createTaskTables();
    derbyConnector.createAuditTable();
    taskStorage = new MetadataTaskStorage(
        derbyConnector,
        new TaskStorageConfig(null),
        new DerbyMetadataStorageActionHandlerFactory(
            derbyConnector,
            derby.metadataTablesConfigSupplier().get(),
            objectMapper
        )
    );
    metadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        testUtils.getTestObjectMapper(),
        derby.metadataTablesConfigSupplier().get(),
        derbyConnector
    );
    taskLockbox = new TaskLockbox(taskStorage);
    final TaskActionToolbox taskActionToolbox = new TaskActionToolbox(
        taskLockbox,
        null,
        metadataStorageCoordinator,
        emitter,
        new SupervisorManager(null)
        {
          @Override
          public boolean checkPointDataSourceMetadata(
              String supervisorId,
              @Nullable String sequenceName,
              @Nullable DataSourceMetadata previousDataSourceMetadata,
              @Nullable DataSourceMetadata currentDataSourceMetadata
          )
          {
            log.info("Adding checkpoint hash to the set");
            checkpointRequestsHash.add(Objects.hash(
                supervisorId,
                sequenceName,
                previousDataSourceMetadata,
                currentDataSourceMetadata
            ));
            return true;
          }
        }
    );
    final TaskActionClientFactory taskActionClientFactory = new LocalTaskActionClientFactory(
        taskStorage,
        taskActionToolbox
    );
    final SegmentHandoffNotifierFactory handoffNotifierFactory = new SegmentHandoffNotifierFactory()
    {
      @Override
      public SegmentHandoffNotifier createSegmentHandoffNotifier(String dataSource)
      {
        return new SegmentHandoffNotifier()
        {
          @Override
          public boolean registerSegmentHandoffCallback(
              SegmentDescriptor descriptor, Executor exec, Runnable handOffRunnable
          )
          {
            if (doHandoff) {
              // Simulate immediate handoff
              exec.execute(handOffRunnable);
            }
            return true;
          }

          @Override
          public void start()
          {
            //Noop
          }

          @Override
          public void close()
          {
            //Noop
          }
        };
      }
    };
    final LocalDataSegmentPusherConfig dataSegmentPusherConfig = new LocalDataSegmentPusherConfig();
    dataSegmentPusherConfig.storageDirectory = getSegmentDirectory();
    final DataSegmentPusher dataSegmentPusher = new LocalDataSegmentPusher(dataSegmentPusherConfig, objectMapper);
    toolboxFactory = new TaskToolboxFactory(
        taskConfig,
        taskActionClientFactory,
        emitter,
        dataSegmentPusher,
        new TestDataSegmentKiller(),
        null, // DataSegmentMover
        null, // DataSegmentArchiver
        new TestDataSegmentAnnouncer(),
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        handoffNotifierFactory,
        makeTimeseriesOnlyConglomerate(),
        Execs.newDirectExecutorService(), // queryExecutorService
        EasyMock.createMock(MonitorScheduler.class),
        new SegmentLoaderFactory(
            new SegmentLoaderLocalCacheManager(
                null,
                new SegmentLoaderConfig()
                {
                  @Override
                  public List<StorageLocationConfig> getLocations()
                  {
                    return Lists.newArrayList();
                  }
                }, testUtils.getTestObjectMapper()
            )
        ),
        testUtils.getTestObjectMapper(),
        testUtils.getTestIndexMerger(),
        testUtils.getTestIndexIO(),
        MapCache.create(1024),
        new CacheConfig(),
        testUtils.getTestIndexMergerV9()
    );
  }

  private void destroyToolboxFactory()
  {
    toolboxFactory = null;
    taskStorage = null;
    taskLockbox = null;
    metadataStorageCoordinator = null;
  }

  private Set<SegmentDescriptor> publishedDescriptors() throws IOException
  {
    return FluentIterable.from(
        metadataStorageCoordinator.getUsedSegmentsForInterval(
            DATA_SCHEMA.getDataSource(),
            new Interval("0000/3000")
        )
    ).transform(
        new Function<DataSegment, SegmentDescriptor>()
        {
          @Override
          public SegmentDescriptor apply(DataSegment input)
          {
            return input.toDescriptor();
          }
        }
    ).toSet();
  }

  private void unlockAppenderatorBasePersistDirForTask(KafkaIndexTask task)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
  {
    Method unlockBasePersistDir = ((AppenderatorImpl) task.getAppenderator()).getClass()
                                                                             .getDeclaredMethod(
                                                                                 "unlockBasePersistDirectory");
    unlockBasePersistDir.setAccessible(true);
    unlockBasePersistDir.invoke(task.getAppenderator());
  }

  private File getSegmentDirectory()
  {
    return new File(directory, "segments");
  }

  private List<String> readSegmentColumn(final String column, final SegmentDescriptor descriptor) throws IOException
  {
    File indexZip = new File(
        StringUtils.format(
            "%s/%s/%s_%s/%s/%d/index.zip",
            getSegmentDirectory(),
            DATA_SCHEMA.getDataSource(),
            descriptor.getInterval().getStart(),
            descriptor.getInterval().getEnd(),
            descriptor.getVersion(),
            descriptor.getPartitionNumber()
        )
    );
    File outputLocation = new File(
        directory,
        StringUtils.format(
            "%s_%s_%s_%s",
            descriptor.getInterval().getStart(),
            descriptor.getInterval().getEnd(),
            descriptor.getVersion(),
            descriptor.getPartitionNumber()
        )
    );
    outputLocation.mkdir();
    CompressionUtils.unzip(
        Files.asByteSource(indexZip),
        outputLocation,
        Predicates.<Throwable>alwaysFalse(),
        false
    );
    IndexIO indexIO = new TestUtils().getTestIndexIO();
    QueryableIndex index = indexIO.loadIndex(outputLocation);
    DictionaryEncodedColumn theColumn = index.getColumn(column).getDictionaryEncoding();
    List<String> values = Lists.newArrayList();
    for (int i = 0; i < theColumn.length(); i++) {
      int id = theColumn.getSingleValueRow(i);
      String value = theColumn.lookupName(id);
      values.add(value);
    }
    return values;
  }

  private List<String> readSegmentDim1(final SegmentDescriptor descriptor) throws IOException
  {
    File indexZip = new File(
        String.format(
            "%s/%s/%s_%s/%s/%d/index.zip",
            getSegmentDirectory(),
            DATA_SCHEMA.getDataSource(),
            descriptor.getInterval().getStart(),
            descriptor.getInterval().getEnd(),
            descriptor.getVersion(),
            descriptor.getPartitionNumber()
        )
    );
    File outputLocation = new File(
        directory,
        String.format(
            "%s_%s_%s_%s",
            descriptor.getInterval().getStart(),
            descriptor.getInterval().getEnd(),
            descriptor.getVersion(),
            descriptor.getPartitionNumber()
        )
    );
    outputLocation.mkdir();
    CompressionUtils.unzip(
        Files.asByteSource(indexZip),
        outputLocation,
        Predicates.<Throwable>alwaysFalse(),
        false
    );
    IndexIO indexIO = new TestUtils().getTestIndexIO();
    QueryableIndex index = indexIO.loadIndex(outputLocation);
    DictionaryEncodedColumn dim1 = index.getColumn("dim1").getDictionaryEncoding();
    List<String> values = Lists.newArrayList();
    for (int i = 0; i < dim1.length(); i++) {
      int id = dim1.getSingleValueRow(i);
      String value = dim1.lookupName(id);
      values.add(value);
    }
    return values;
  }

  public long countEvents(final Task task) throws Exception
  {
    // Do a query.
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(DATA_SCHEMA.getDataSource())
                                  .aggregators(
                                      ImmutableList.<AggregatorFactory>of(
                                          new LongSumAggregatorFactory("rows", "rows")
                                      )
                                  ).granularity(QueryGranularities.ALL)
                                  .intervals("0000/3000")
                                  .build();

    ArrayList<Row> results = Sequences.toList(
        task.getQueryRunner(query).run(query, ImmutableMap.<String, Object>of()),
        Lists.<Row>newArrayList()
    );

    return results.isEmpty() ? 0 : results.get(0).getLongMetric("rows");
  }

  private static byte[] JB(String timestamp, String dim1, String dim2, double met1)
  {
    try {
      return new ObjectMapper().writeValueAsBytes(
          ImmutableMap.of("timestamp", timestamp, "dim1", dim1, "dim2", dim2, "met1", met1)
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private SegmentDescriptor SD(final Task task, final String intervalString, final int partitionNum)
  {
    final Interval interval = new Interval(intervalString);
    return new SegmentDescriptor(
        DATA_SCHEMA.getDataSource(),
        interval,
        getLock(task, interval).getVersion(),
        partitionNum
    );
  }
}
