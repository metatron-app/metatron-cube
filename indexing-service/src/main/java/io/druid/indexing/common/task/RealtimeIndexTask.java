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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.common.Progressing;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.FirehoseFactoryV2;
import io.druid.data.input.FirehoseV2;
import io.druid.indexer.TaskStatus;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.LockAcquireAction;
import io.druid.indexing.common.actions.LockReleaseAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.query.DruidMetrics;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunners;
import io.druid.query.QueryToolChest;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.RealtimeMetricsMonitor;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.segment.realtime.firehose.ClippedFirehoseFactory;
import io.druid.segment.realtime.firehose.EventReceiverFirehoseFactory;
import io.druid.segment.realtime.firehose.FirehoseV2CloseBeforeStartException;
import io.druid.segment.realtime.firehose.TimedShutoffFirehoseFactory;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.PlumberSchool;
import io.druid.segment.realtime.plumber.Plumbers;
import io.druid.segment.realtime.plumber.RealtimePlumberSchool;
import io.druid.segment.realtime.plumber.VersioningPolicy;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class RealtimeIndexTask extends AbstractTask
{
  // to notify ready signal to overlord
  private static final Interval READY = new Interval(JodaUtils.MAX_INSTANT, JodaUtils.MAX_INSTANT);

  private static final EmittingLogger log = new EmittingLogger(RealtimeIndexTask.class);
  private final static Random random = new Random();

  private static String makeTaskId(FireDepartment fireDepartment)
  {
    return makeTaskId(
        fireDepartment.getDataSchema().getDataSource(),
        fireDepartment.getTuningConfig().getShardSpec().getPartitionNum(),
        new DateTime(),
        random.nextInt()
    );
  }

  static String makeTaskId(String dataSource, int partitionNumber, DateTime timestamp, int randomBits)
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Integer.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((randomBits >>> (i * 4)) & 0x0F)));
    }
    return String.format(
        "index_realtime_%s_%d_%s_%s",
        dataSource,
        partitionNumber,
        timestamp,
        suffix
    );
  }

  private static String makeDatasource(FireDepartment fireDepartment)
  {
    return fireDepartment.getDataSchema().getDataSource();
  }

  @JsonIgnore
  private final FireDepartment spec;

  @JsonIgnore
  private final ObjectMapper jsonMapper;

  @JsonIgnore
  private volatile Plumber plumber = null;

  @JsonIgnore
  private volatile Firehose firehose = null;

  @JsonIgnore
  private volatile FirehoseV2 firehoseV2 = null;

  @JsonIgnore
  private volatile FireDepartmentMetrics metrics = null;

  @JsonIgnore
  private volatile boolean gracefullyStopped = false;

  @JsonIgnore
  private volatile boolean finishingJob = false;

  @JsonIgnore
  private volatile Thread runThread = null;

  @JsonIgnore
  private volatile QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate = null;

  @JsonIgnore
  private long startTime;

  @JsonCreator
  public RealtimeIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") FireDepartment fireDepartment,
      @JacksonInject ObjectMapper jsonMapper,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        id == null ? makeTaskId(fireDepartment) : id,
        String.format("index_realtime_%s", makeDatasource(fireDepartment)),
        taskResource,
        makeDatasource(fireDepartment),
        context
    );
    this.jsonMapper = jsonMapper;
    this.spec = fireDepartment;
  }

  @Override
  public String getType()
  {
    return "index_realtime";
  }

  @Override
  public String getNodeType()
  {
    return "realtime";
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    if (plumber != null) {
      QueryRunnerFactory<T> factory = queryRunnerFactoryConglomerate.findFactory(query);
      QueryToolChest<T> toolChest = factory.getToolchest();

      return QueryRunners.finalizeAndPostProcessing(plumber.getQueryRunner(query), toolChest, jsonMapper);
    } else {
      return null;
    }
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return true;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    runThread = Thread.currentThread();

    if (plumber != null) {
      throw new IllegalStateException("Should not run with plumber");
    }
    startTime = System.currentTimeMillis();

    // It would be nice to get the PlumberSchool in the constructor.  Although that will need jackson injectables for
    // stuff like the ServerView, which seems kind of odd?  Perhaps revisit this when Guice has been introduced.

    final SegmentPublisher segmentPublisher = new TaskActionSegmentPublisher(this, toolbox);

    // NOTE: We talk to the coordinator in various places in the plumber and we could be more robust to issues
    // with the coordinator.  Right now, we'll block/throw in whatever thread triggered the coordinator behavior,
    // which will typically be either the main data processing loop or the persist thread.

    // Wrap default DataSegmentAnnouncer such that we unlock intervals as we unannounce segments
    final DataSegmentAnnouncer lockingSegmentAnnouncer = new DataSegmentAnnouncer()
    {
      @Override
      public void announceSegment(final DataSegment segment) throws IOException
      {
        // Side effect: Calling announceSegment causes a lock to be acquired
        toolbox.getTaskActionClient().submit(new LockAcquireAction(segment.getInterval()));
        toolbox.getSegmentAnnouncer().announceSegment(segment);
      }

      @Override
      public void unannounceSegment(final DataSegment segment) throws IOException
      {
        try {
          toolbox.getSegmentAnnouncer().unannounceSegment(segment);
        }
        finally {
          toolbox.getTaskActionClient().submit(new LockReleaseAction(segment.getInterval()));
        }
      }

      @Override
      public void announceSegments(Iterable<DataSegment> segments) throws IOException
      {
        // Side effect: Calling announceSegments causes locks to be acquired
        for (DataSegment segment : segments) {
          toolbox.getTaskActionClient().submit(new LockAcquireAction(segment.getInterval()));
        }
        toolbox.getSegmentAnnouncer().announceSegments(segments);
      }

      @Override
      public void unannounceSegments(Iterable<DataSegment> segments) throws IOException
      {
        try {
          toolbox.getSegmentAnnouncer().unannounceSegments(segments);
        }
        finally {
          for (DataSegment segment : segments) {
            toolbox.getTaskActionClient().submit(new LockReleaseAction(segment.getInterval()));
          }
        }
      }
    };

    // NOTE: getVersion will block if there is lock contention, which will block plumber.getSink
    // NOTE: (and thus the firehose)

    // Shouldn't usually happen, since we don't expect people to submit tasks that intersect with the
    // realtime window, but if they do it can be problematic. If we decide to care, we can use more threads in
    // the plumber such that waiting for the coordinator doesn't block data processing.
    final VersioningPolicy versioningPolicy = new VersioningPolicy()
    {
      @Override
      public String getVersion(final Interval interval)
      {
        try {
          // Side effect: Calling getVersion causes a lock to be acquired
          final TaskLock myLock = toolbox.getTaskActionClient()
                                         .submit(new LockAcquireAction(interval));

          return myLock.getVersion();
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    };

    DataSchema dataSchema = spec.getDataSchema();
    RealtimeIOConfig realtimeIOConfig = spec.getIOConfig();
    RealtimeTuningConfig tuningConfig = spec.getTuningConfig()
                                            .withBasePersistDirectory(new File(toolbox.getTaskWorkDir(), "persist"))
                                            .withVersioningPolicy(versioningPolicy);

    final FireDepartment fireDepartment = new FireDepartment(
        dataSchema,
        realtimeIOConfig,
        tuningConfig
    );
    this.metrics = fireDepartment.getMetrics();
    final RealtimeMetricsMonitor metricsMonitor = new RealtimeMetricsMonitor(
        ImmutableList.of(fireDepartment),
        ImmutableMap.of(
            DruidMetrics.TASK_ID, new String[]{getId()}
        )
    );
    this.queryRunnerFactoryConglomerate = toolbox.getQueryRunnerFactoryConglomerate();

    // NOTE: This pusher selects path based purely on global configuration and the DataSegment, which means
    // NOTE: that redundant realtime tasks will upload to the same location. This can cause index.zip and
    // NOTE: descriptor.json to mismatch, or it can cause historical nodes to load different instances of the
    // NOTE: "same" segment.
    final PlumberSchool plumberSchool = new RealtimePlumberSchool(
        toolbox.getEmitter(),
        toolbox.getQueryRunnerFactoryConglomerate(),
        toolbox.getSegmentPusher(),
        lockingSegmentAnnouncer,
        segmentPublisher,
        toolbox.getSegmentHandoffNotifierFactory(),
        toolbox.getQueryExecutorService(),
        toolbox.getIndexMerger(),
        toolbox.getIndexMergerV9(),
        toolbox.getIndexIO(),
        toolbox.getCache(),
        toolbox.getCacheConfig(),
        toolbox.getObjectMapper()
    );

    this.plumber = plumberSchool.findPlumber(dataSchema, tuningConfig, metrics);

    Supplier<Committer> committerSupplier = null;

    try {
      toolbox.getDataSegmentServerAnnouncer().announce();

      Object metadata = plumber.startJob();

      // Set up metrics emission
      toolbox.getMonitorScheduler().addMonitor(metricsMonitor);

      if (fireDepartment.checkFirehoseV2())
      {
        final boolean firehoseV2DrainableByClosing =
            isFirehoseV2DrainableByClosing(spec.getIOConfig().getFirehoseFactoryV2());
        boolean normalStart = true;

        // Skip connecting firehose if we've been stopped before we got started.
        synchronized (this) {
          if (!gracefullyStopped) {
            firehoseV2 = fireDepartment.connect(toolbox.getObjectMapper(), metadata);
            committerSupplier = Committers.supplierFromFirehoseV2(firehoseV2);
            try {
              firehoseV2.start();
            }
            catch (FirehoseV2CloseBeforeStartException e) {
              normalStart = false;
            }
          }
        }

        if (normalStart) {

          // notify ready
          toolbox.getTaskActionClient().submit(new LockAcquireAction(READY));

          // Time to read data!
          while (firehoseV2 != null && (!gracefullyStopped || firehoseV2DrainableByClosing))
          {
            Plumbers.addNextRowV2(
                committerSupplier,
                firehoseV2,
                plumber,
                tuningConfig.isReportParseExceptions(),
                metrics
            );

            if (!firehoseV2.advance()) break;
          }
        }
      } else {
        // Delay firehose connection to avoid claiming input resources while the plumber is starting up.
        final boolean firehoseDrainableByClosing =
            isFirehoseDrainableByClosing(spec.getIOConfig().getFirehoseFactory());

        // Skip connecting firehose if we've been stopped before we got started.
        synchronized (this) {
          if (!gracefullyStopped) {
            firehose = fireDepartment.connect(toolbox.getObjectMapper());
            committerSupplier = Committers.supplierFromFirehose(firehose);
          }
        }

        // notify ready
        toolbox.getTaskActionClient().submit(new LockAcquireAction(READY));

        // Time to read data!
        while (firehose != null && (!gracefullyStopped || firehoseDrainableByClosing) && firehose.hasMore()) {
          Plumbers.addNextRow(
              committerSupplier,
              firehose,
              plumber,
              tuningConfig.isReportParseExceptions(),
              metrics
          );
        }
      }
    }
    catch (Throwable e) {
      log.makeAlert(e, "Exception aborted realtime processing[%s]", dataSchema.getDataSource())
         .emit();
      throw e;
    }

    try {
      // Persist if we had actually started.
      if (firehose != null || firehoseV2 != null) {
        log.info("Persisting remaining data.");

        final Committer committer = committerSupplier.get();
        final CountDownLatch persistLatch = new CountDownLatch(1);
        plumber.persist(
            new Committer()
            {
              @Override
              public Object getMetadata()
              {
                return committer.getMetadata();
              }

              @Override
              public void run()
              {
                try {
                  committer.run();
                }
                finally {
                  persistLatch.countDown();
                }
              }
            }
        );
        persistLatch.await();
      }

      if (gracefullyStopped) {
        log.info("Gracefully stopping.");
      } else {
        log.info("Finishing the job.");
        synchronized (this) {
          if (gracefullyStopped) {
            // Someone called stopGracefully after we checked the flag. That's okay, just stop now.
            log.info("Gracefully stopping.");
          } else {
            finishingJob = true;
          }
        }

        if (finishingJob) {
          plumber.finishJob();
        }
      }

      toolbox.getDataSegmentServerAnnouncer().unannounce();
    }
    catch (InterruptedException e) {
      log.debug(e, "Interrupted while finishing the job");
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to finish realtime task").emit();
      throw e;
    }
    finally {
      if (firehose != null) {
        CloseQuietly.close(firehose);
      }
      if (firehoseV2 != null) {
        CloseQuietly.close(firehoseV2);
      }
      toolbox.getMonitorScheduler().removeMonitor(metricsMonitor);
    }

    log.info("Job done! %,d msec", (System.currentTimeMillis() - startTime));
    return TaskStatus.success(getId());
  }

  @Override
  public boolean canRestore()
  {
    return true;
  }

  @Override
  public void stopGracefully()
  {
    try {
      synchronized (this) {
        if (!gracefullyStopped) {
          gracefullyStopped = true;
          if (firehose == null && firehoseV2 == null) {
            log.info("stopGracefully: Firehose not started yet, so nothing to stop.");
          } else if (finishingJob) {
            log.info("stopGracefully: Interrupting finishJob.");
            runThread.interrupt();
          } else if (firehose != null && isFirehoseDrainableByClosing(spec.getIOConfig().getFirehoseFactory())) {
            log.info("stopGracefully: Draining firehose.");
            firehose.close();
          } else if (firehoseV2 != null && isFirehoseV2DrainableByClosing(spec.getIOConfig().getFirehoseFactoryV2())) {
            log.info("stopGracefully: Draining firehoseV2.");
            firehoseV2.close();
          } else {
            log.info("stopGracefully: Cannot drain firehose by closing, interrupting run thread.");
            runThread.interrupt();
          }
        }
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Public for tests.
   */
  @JsonIgnore
  public Firehose getFirehose()
  {
    return firehose;
  }

  /**
   * Public for tests.
   */
  @JsonIgnore
  public FirehoseV2 getFirehoseV2()
  {
    return firehoseV2;
  }

  /**
   * Public for tests.
   */
  @JsonIgnore
  public FireDepartmentMetrics getMetrics()
  {
    return metrics;
  }

  @JsonProperty("spec")
  public FireDepartment getRealtimeIngestionSchema()
  {
    return spec;
  }

  @Override
  public float progress()
  {
    float progress = -1;
    if (firehose instanceof Progressing) {
      progress = ((Progressing) firehose).progress();
    } else if (firehoseV2 instanceof Progressing) {
      progress = ((Progressing) firehoseV2).progress();
    }
    if (plumber == null || progress <= 0) {
      return progress;
    }
    long estimated = Plumbers.estimatedFinishTime(plumber);
    if (estimated >= 0) {
      long elapsed = System.currentTimeMillis() - startTime;
      return elapsed * progress / (elapsed + estimated);
    }
    return progress * 0.66f;  // assume 1/3 for persisting / handoff time
  }

  /**
   * Is a firehose from this factory drainable by closing it? If so, we should drain on stopGracefully rather than
   * abruptly stopping.
   *
   * This is a hack to get around the fact that the Firehose and FirehoseFactory interfaces do not help us do this.
   *
   * Protected for tests.
   */
  protected boolean isFirehoseDrainableByClosing(FirehoseFactory firehoseFactory)
  {
    return firehoseFactory instanceof EventReceiverFirehoseFactory
           || (firehoseFactory instanceof TimedShutoffFirehoseFactory
               && isFirehoseDrainableByClosing(((TimedShutoffFirehoseFactory) firehoseFactory).getDelegateFactory()))
           || (firehoseFactory instanceof ClippedFirehoseFactory
               && isFirehoseDrainableByClosing(((ClippedFirehoseFactory) firehoseFactory).getDelegate()));
  }

  /**
   * Is a firehoseV2 from this factory drainable by closing it? If so, we should drain on stopGracefully rather than
   * abruptly stopping.
   *
   * This is a hack to get around the fact that the FirehoseV2 and FirehoseFactoryV2 interfaces do not help us do this.
   * And, currently no FirehoseFactoryV2 implementation supports drainable by closing yet.
   * Protected for tests.
   */
  protected boolean isFirehoseV2DrainableByClosing(FirehoseFactoryV2 firehoseFactoryV2)
  {
    return false;
  }

  public static class TaskActionSegmentPublisher implements SegmentPublisher
  {
    final Task task;
    final TaskToolbox taskToolbox;

    public TaskActionSegmentPublisher(Task task, TaskToolbox taskToolbox)
    {
      this.task = task;
      this.taskToolbox = taskToolbox;
    }

    @Override
    public void publishSegment(DataSegment segment) throws IOException
    {
      taskToolbox.publishSegments(ImmutableList.of(segment));
    }
  }
}
