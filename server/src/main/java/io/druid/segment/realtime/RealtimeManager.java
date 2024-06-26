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

package io.druid.segment.realtime;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.concurrent.Execs;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseV2;
import io.druid.data.input.InputRow;
import io.druid.guice.annotations.Processing;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.query.ForwardingSegmentWalker;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.QueryRunners;
import io.druid.query.QueryToolChest;
import io.druid.query.SegmentDescriptor;
import io.druid.query.StorageHandler;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.Plumbers;
import io.druid.server.ForwardHandler;
import io.druid.server.coordination.DataSegmentServerAnnouncer;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class RealtimeManager implements ForwardingSegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(RealtimeManager.class);

  private final List<FireDepartment> fireDepartments;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ForwardHandler forwardHandler;
  private final ObjectMapper objectMapper;
  private final ExecutorService executor;
  private final DataSegmentServerAnnouncer serverAnnouncer;

  /**
   * key=data source name,value=mappings of partition number to FireChief
   */
  private final Map<String, Map<Integer, FireChief>> chiefs;

  @Inject
  public RealtimeManager(
      List<FireDepartment> fireDepartments,
      QueryRunnerFactoryConglomerate conglomerate,
      ForwardHandler forwardHandler,
      ObjectMapper objectMapper,
      @Processing ExecutorService executor,
      DataSegmentServerAnnouncer serverAnnouncer
  )
  {
    this(fireDepartments, conglomerate, serverAnnouncer, forwardHandler, objectMapper, executor, Maps.newHashMap());
  }

  RealtimeManager(
      List<FireDepartment> fireDepartments,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentServerAnnouncer serverAnnouncer,
      ForwardHandler forwardHandler,
      ObjectMapper objectMapper,
      ExecutorService executor,
      Map<String, Map<Integer, FireChief>> chiefs
  )
  {
    this.fireDepartments = fireDepartments;
    this.conglomerate = conglomerate;
    this.serverAnnouncer = serverAnnouncer;
    this.forwardHandler = forwardHandler;
    this.objectMapper = objectMapper;
    this.executor = executor;
    this.chiefs = chiefs;
  }

  @LifecycleStart
  public void start() throws IOException
  {
    serverAnnouncer.announce();

    for (final FireDepartment fireDepartment : fireDepartments) {
      final DataSchema schema = fireDepartment.getDataSchema();

      final FireChief chief = new FireChief(fireDepartment, conglomerate, objectMapper);
      Map<Integer, FireChief> partitionChiefs = chiefs.get(schema.getDataSource());
      if (partitionChiefs == null) {
        partitionChiefs = new HashMap<>();
        chiefs.put(schema.getDataSource(), partitionChiefs);
      }
      partitionChiefs.put(fireDepartment.getTuningConfig().getShardSpec().getPartitionNum(), chief);

      chief.setName(
          String.format(
              "chief-%s[%s]",
              schema.getDataSource(),
              fireDepartment.getTuningConfig().getShardSpec().getPartitionNum()
          )
      );
      chief.setDaemon(true);
      chief.start();
    }
  }

  @LifecycleStop
  public void stop()
  {
    for (Map<Integer, FireChief> chiefs : this.chiefs.values()) {
      for (FireChief chief : chiefs.values()) {
        CloseQuietly.close(chief);
      }
    }

    serverAnnouncer.unannounce();
  }

  public FireDepartmentMetrics getMetrics(String datasource)
  {
    Map<Integer, FireChief> chiefs = this.chiefs.get(datasource);
    if (chiefs == null) {
      return null;
    }
    FireDepartmentMetrics snapshot = null;
    for (FireChief chief : chiefs.values()) {
      if (snapshot == null) {
        snapshot = chief.getMetrics().snapshot();
      } else {
        snapshot.merge(chief.getMetrics());
      }
    }
    return snapshot;
  }

  @Override
  public QueryConfig getConfig()
  {
    return conglomerate.getConfig();
  }

  @Override
  public ExecutorService getExecutor()
  {
    return executor;
  }

  @Override
  public ObjectMapper getMapper()
  {
    return objectMapper;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, Iterable<Interval> intervals)
  {
    if (query instanceof Query.ManagementQuery) {
      return QueryRunnerHelper.toManagementRunner(query, conglomerate, null, objectMapper);
    }
    final QueryRunnerFactory<T> factory = conglomerate.findFactory(query);
    final Map<Integer, FireChief> partitionChiefs = chiefs.get(Iterables.getOnlyElement(query.getDataSource()
                                                                                             .getNames()));

    return partitionChiefs == null ? NoopQueryRunner.instance() : factory.getToolchest().mergeResults(
        factory.mergeRunners(
            query,
            Execs.newDirectExecutorService(),
            // Chaining query runners which wait on submitted chain query runners can make executor pools deadlock
            Iterables.transform(
                partitionChiefs.values(), new Function<FireChief, QueryRunner<T>>()
                {
                  @Override
                  public QueryRunner<T> apply(FireChief fireChief)
                  {
                    return fireChief.getQueryRunner(query);
                  }
                }
            ),
            null
        )
    );
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    if (query instanceof Query.ManagementQuery) {
      return QueryRunnerHelper.toManagementRunner(query, conglomerate, null, objectMapper);
    }
    final QueryRunnerFactory<T> factory = conglomerate.findFactory(query);
    final Map<Integer, FireChief> partitionChiefs = chiefs.get(
        Iterables.getOnlyElement(
            query.getDataSource()
                 .getNames()
        )
    );

    return partitionChiefs == null
           ? NoopQueryRunner.instance()
           : factory.getToolchest().mergeResults(
               factory.mergeRunners(
                   query,
                   Execs.newDirectExecutorService(),
                   Iterables.transform(
                       specs,
                       new Function<SegmentDescriptor, QueryRunner<T>>()
                       {
                         @Override
                         public QueryRunner<T> apply(SegmentDescriptor spec)
                         {
                           final FireChief retVal = partitionChiefs.get(spec.getPartitionNumber());
                           return retVal == null
                                  ? NoopQueryRunner.instance()
                                  : retVal.getQueryRunner(query.withQuerySegmentSpec(new SpecificSegmentSpec(spec)));
                         }
                       }
                   ),
                   null
               )
           );
  }

  static class FireChief extends Thread implements Closeable
  {
    private final FireDepartment fireDepartment;
    private final FireDepartmentMetrics metrics;
    private final RealtimeTuningConfig config;
    private final QueryRunnerFactoryConglomerate conglomerate;
    private final ObjectMapper mapper;

    private volatile Firehose firehose = null;
    private volatile FirehoseV2 firehoseV2 = null;
    private volatile Plumber plumber = null;
    private volatile boolean normalExit = true;

    public FireChief(FireDepartment fireDepartment, QueryRunnerFactoryConglomerate conglomerate, ObjectMapper mapper)
    {
      this.fireDepartment = fireDepartment;
      this.conglomerate = conglomerate;
      this.mapper = mapper;
      this.config = fireDepartment.getTuningConfig();
      this.metrics = fireDepartment.getMetrics();
    }

    public Firehose initFirehose()
    {
      synchronized (this) {
        if (firehose == null) {
          try {
            log.info("Calling the FireDepartment and getting a Firehose.");
            firehose = fireDepartment.connect(mapper);
            log.info("Firehose acquired!");
          }
          catch (IOException e) {
            throw Throwables.propagate(e);
          }
        } else {
          log.warn("Firehose already connected, skipping initFirehose().");
        }

        return firehose;
      }
    }

    public FirehoseV2 initFirehoseV2(Object metaData)
    {
      synchronized (this) {
        if (firehoseV2 == null) {
          try {
            log.info("Calling the FireDepartment and getting a FirehoseV2.");
            firehoseV2 = fireDepartment.connect(mapper, metaData);
            log.info("FirehoseV2 acquired!");
          }
          catch (IOException e) {
            throw Throwables.propagate(e);
          }
        } else {
          log.warn("FirehoseV2 already connected, skipping initFirehoseV2().");
        }

        return firehoseV2;
      }
    }

    public Plumber initPlumber()
    {
      synchronized (this) {
        if (plumber == null) {
          log.info("Someone get us a plumber!");
          plumber = fireDepartment.findPlumber();
          log.info("We have our plumber!");
        } else {
          log.warn("Plumber already trained, skipping initPlumber().");
        }

        return plumber;
      }
    }

    public FireDepartmentMetrics getMetrics()
    {
      return metrics;
    }

    @Override
    public void run()
    {
      plumber = initPlumber();

      try {
        Object metadata = plumber.startJob();

        if (fireDepartment.checkFirehoseV2()) {
          firehoseV2 = initFirehoseV2(metadata);
          runFirehoseV2(firehoseV2);
        } else {
          firehose = initFirehose();
          runFirehose(firehose);
        }

      }
      catch (RuntimeException e) {
        log.makeAlert(
            e,
            "RuntimeException aborted realtime processing[%s]",
            fireDepartment.getDataSchema().getDataSource()
        ).emit();
        normalExit = false;
        throw e;
      }
      catch (Error e) {
        log.makeAlert(e, "Exception aborted realtime processing[%s]", fireDepartment.getDataSchema().getDataSource())
           .emit();
        normalExit = false;
        throw e;
      }
      finally {
        CloseQuietly.close(firehose);
        if (normalExit) {
          plumber.finishJob();
          plumber = null;
          firehose = null;
        }
      }
    }

    private void runFirehoseV2(FirehoseV2 firehose)
    {
      try {
        firehose.start();
      }
      catch (Exception e) {
        log.error(e, "Failed to start firehoseV2");
        return;
      }

      log.info("FirehoseV2 started");
      final Supplier<Committer> committerSupplier = Committers.supplierFromFirehoseV2(firehose);
      boolean haveRow = true;
      while (haveRow) {
        InputRow inputRow = null;
        int numRows = 0;
        try {
          inputRow = firehose.currRow();
          if (inputRow != null) {
            numRows = plumber.add(inputRow, committerSupplier);
            if (numRows < 0) {
              metrics.incrementThrownAway();
              log.debug("Throwing away event[%s]", inputRow);
            } else {
              metrics.incrementProcessed();
            }
          } else {
            log.debug("thrown away null input row, considering unparseable");
            metrics.incrementUnparseable();
          }
        }
        catch (Exception e) {
          log.makeAlert(e, "Unknown exception, Ignoring and continuing.")
             .addData("inputRow", inputRow);
        }

        try {
          haveRow = firehose.advance();
        }
        catch (Exception e) {
          log.debug(e, "exception in firehose.advance(), considering unparseable row");
          metrics.incrementUnparseable();
        }
      }
    }

    private void runFirehose(Firehose firehose)
    {
      final Supplier<Committer> committerSupplier = Committers.supplierFromFirehose(firehose);
      while (firehose.hasMore()) {
        Plumbers.addNextRow(committerSupplier, firehose, plumber, config.isReportParseExceptions(), metrics);
      }
    }

    public <T> QueryRunner<T> getQueryRunner(Query<T> query)
    {
      QueryRunnerFactory<T> factory = conglomerate.findFactory(query);
      QueryToolChest<T> toolChest = factory.getToolchest();

      return QueryRunners.finalizeAndPostProcessing(plumber.getQueryRunner(query), toolChest, mapper);
    }

    public void close() throws IOException
    {
      synchronized (this) {
        if (firehose != null) {
          normalExit = false;
          firehose.close();
        }
      }
    }
  }

  @Override
  public StorageHandler getHandler(String scheme)
  {
    return forwardHandler.getHandler(scheme);
  }

  @Override
  public <T> QueryRunner<T> handle(Query<T> query, QueryRunner<T> baseRunner)
  {
    return forwardHandler.wrapForward(query, baseRunner);
  }
}
