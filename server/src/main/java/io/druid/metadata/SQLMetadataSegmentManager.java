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

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import io.druid.client.DruidDataSource;
import io.druid.common.DateTimes;
import io.druid.common.utils.JodaUtils;
import io.druid.concurrent.Execs;
import io.druid.guice.ManageLifecycle;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.skife.jdbi.v2.BaseResultSetMapper;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder2;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.IntegerMapper;
import org.skife.jdbi.v2.util.StringMapper;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
@ManageLifecycle
public class SQLMetadataSegmentManager implements MetadataSegmentManager
{
  private static final EmittingLogger log = new EmittingLogger(SQLMetadataSegmentManager.class);


  private final State lock = new State();

  private final ObjectMapper jsonMapper;
  private final Supplier<MetadataSegmentManagerConfig> config;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final Map<String, DruidDataSource> dataSources;
  private final IndexerSQLMetadataStorageCoordinator metaDataCoordinator;
  private final SQLMetadataConnector connector;

  private volatile ListeningScheduledExecutorService exec = null;

  private static class State
  {
    private boolean started;
    private DateTime lastUpdatedTime;

    private boolean kicked;

    private synchronized void kick()
    {
      kicked = true;
      notifyAll();
    }

    private synchronized boolean next(long msec)
    {
      long remaining = msec;
      while (started && remaining > 0 && !kicked) {
        long start = System.currentTimeMillis();
        try {
          wait(remaining);
        }
        catch (InterruptedException e) {
          // ignore
        }
        remaining -= System.currentTimeMillis() - start;
      }
      kicked = false;
      return started;
    }

    private synchronized boolean stop()
    {
      final boolean running = started;
      started = false;
      lastUpdatedTime = null;
      return running;
    }

    private synchronized boolean isStarted()
    {
      return started;
    }

    private synchronized DateTime lastUpdatedTime()
    {
      return lastUpdatedTime;
    }
  }

  @Inject
  public SQLMetadataSegmentManager(
      ObjectMapper jsonMapper,
      Supplier<MetadataSegmentManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      IndexerSQLMetadataStorageCoordinator metaDataCoordinator,
      SQLMetadataConnector connector
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbTables = dbTables;
    this.dataSources = new HashMap<String, DruidDataSource>();
    this.metaDataCoordinator = metaDataCoordinator;
    this.connector = connector;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (lock.started) {
        return;
      }

      final Duration delay = config.get().getPollDuration().toStandardDuration();
      Preconditions.checkArgument(delay.getMillis() > 0);

      String name = String.format("MetadataSegmentManager(%s)", delay);
      exec = MoreExecutors.listeningDecorator(Execs.scheduledSingleThreaded(name));
      exec.execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              while (lock.next(delay.getMillis())) {
                try {
                  poll();
                }
                catch (Throwable t) {
                  log.makeAlert(t, "uncaught exception in segment manager polling thread").emit();
                }
              }
            }
          }
      );
      lock.started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (lock.stop()) {
      synchronized (dataSources) {
        dataSources.clear();
      }
      exec.shutdownNow();
      exec = null;
    }
  }

  private <T> T inReadOnlyTransaction(final TransactionCallback<T> callback)
  {
    return connector.getDBI().withHandle(
        new HandleCallback<T>()
        {
          @Override
          public T withHandle(Handle handle) throws Exception
          {
            final Connection connection = handle.getConnection();
            final boolean readOnly = connection.isReadOnly();
            connection.setReadOnly(true);
            try {
              return handle.inTransaction(callback);
            } finally {
              try {
                connection.setReadOnly(readOnly);
              } catch (SQLException e) {
                // at least try to log it so we don't swallow exceptions
                log.error(e, "Unable to reset connection read-only state");
              }
            }
          }
        }
    );
  }

  @Override
  public boolean enableDatasource(final String ds, final boolean now)
  {
    try {
      final IDBI dbi = connector.getDBI();
      VersionedIntervalTimeline<String, DataSegment> segmentTimeline = inReadOnlyTransaction(
          new TransactionCallback<VersionedIntervalTimeline<String, DataSegment>>()
          {
            @Override
            public VersionedIntervalTimeline<String, DataSegment> inTransaction(
                Handle handle, TransactionStatus status
            ) throws Exception
            {
              return handle
                  .createQuery(String.format(
                      "SELECT payload FROM %s WHERE dataSource = :dataSource",
                      getSegmentsTable()
                  ))
                  .setFetchSize(connector.getStreamingFetchSize())
                  .bind("dataSource", ds)
                  .map(ByteArrayMapper.FIRST)
                  .fold(
                      new VersionedIntervalTimeline<String, DataSegment>(Ordering.natural()),
                      new Folder3<VersionedIntervalTimeline<String, DataSegment>, byte[]>()
                      {
                        @Override
                        public VersionedIntervalTimeline<String, DataSegment> fold(
                            VersionedIntervalTimeline<String, DataSegment> timeline,
                            byte[] payload,
                            FoldController foldController,
                            StatementContext statementContext
                        ) throws SQLException
                        {
                          try {
                            DataSegment segment = jsonMapper.readValue(
                                payload,
                                DataSegment.class
                            );

                            timeline.add(
                                segment.getInterval(),
                                segment.getVersion(),
                                segment.getShardSpecWithDefault().createChunk(segment)
                            );

                            return timeline;
                          }
                          catch (Exception e) {
                            throw new SQLException(e.toString());
                          }
                        }
                      }
                  );
            }
          }
      );

      final List<DataSegment> segments = Lists.newArrayList();
      for (TimelineObjectHolder<String, DataSegment> objectHolder : segmentTimeline.lookup(
          new Interval(
              "0000-01-01/3000-01-01"
          )
      )) {
        for (PartitionChunk<DataSegment> partitionChunk : objectHolder.getObject()) {
          segments.add(partitionChunk.getObject());
        }
      }

      if (segments.isEmpty()) {
        log.warn("No segments found in the database!");
        return false;
      }

      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              Batch batch = handle.createBatch();

              for (DataSegment segment : segments) {
                batch.add(
                    String.format(
                        "UPDATE %s SET used=true WHERE id = '%s'",
                        getSegmentsTable(),
                        segment.getIdentifier()
                    )
                );
              }
              batch.execute();

              return null;
            }
          }
      );
    }
    catch (Exception e) {
      log.error(e, "Exception enabling datasource %s", ds);
      return false;
    }
    if (now) {
      lock.kick();
    }

    return true;
  }

  @Override
  public boolean registerToView(DataSegment segment)
  {
    synchronized (dataSources) {
      return dataSources.computeIfAbsent(segment.getDataSource(), DruidDataSource.FACTORY).addSegmentIfAbsent(segment);
    }
  }

  @Override
  public boolean unregisterFromView(DataSegment segment)
  {
    final DruidDataSource dataSource = getInventoryValue(segment.getDataSource());
    return dataSource != null && dataSource.removeSegment(segment.getIdentifier());
  }

  @Override
  public boolean isAvailable(DataSegment segment)
  {
    final DruidDataSource ds = getInventoryValue(segment.getDataSource());
    return ds != null && ds.contains(segment.getIdentifier());
  }

  @Override
  public boolean enableSegment(final String segmentId, final boolean now)
  {
    try {
      connector.getDBI().withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              handle.createStatement(
                  String.format("UPDATE %s SET used=true WHERE id = :id", getSegmentsTable())
              )
                    .bind("id", segmentId)
                    .execute();
              return null;
            }
          }
      );
    }
    catch (Exception e) {
      log.error(e, "Exception enabling segment %s", segmentId);
      return false;
    }
    if (now) {
      lock.kick();
    }

    return true;
  }

  @Override
  public boolean disableDatasource(final String ds)
  {
    final String sql = String.format("UPDATE %s SET used=false WHERE dataSource = :dataSource", getSegmentsTable());

    try {
      int update = connector.getDBI().withHandle(
          new HandleCallback<Integer>()
          {
            @Override
            public Integer withHandle(Handle handle) throws Exception
            {
              return handle.createStatement(sql)
                           .bind("dataSource", ds)
                           .execute();
            }
          }
      );
      synchronized (dataSources) {
        dataSources.remove(ds);
      }
      return update > 0;
    }
    catch (Exception e) {
      log.error(e, "Error removing datasource %s", ds);
      return false;
    }
  }

  @Override
  public Pair<String, DataSegment> getLastUpdatedSegment(final String ds)
  {
    final String statement = String.format(
        "SELECT created_date, payload FROM %s WHERE dataSource = :dataSource AND used=true" +
        " ORDER BY created_date DESC limit 1",
        getSegmentsTable()
    );
    return inReadOnlyTransaction(
        new TransactionCallback<Pair<String, DataSegment>>()
        {
          @Override
          public Pair<String, DataSegment> inTransaction(Handle handle, TransactionStatus status) throws Exception
          {
            return handle
                .createQuery(statement)
                .bind("dataSource", ds)
                .map(
                    new ResultSetMapper<Pair<String, DataSegment>>()
                    {
                      @Override
                      public Pair<String, DataSegment> map(int index, ResultSet r, StatementContext ctx)
                          throws SQLException
                      {
                        try {
                          return Pair.of(
                              r.getString("created_date"),
                              jsonMapper.readValue(r.getBytes("payload"), DataSegment.class)
                          );
                        }
                        catch (IOException e) {
                          log.makeAlert(e, "Failed to read segment from db.");
                          return null;
                        }
                      }
                    }
                )
                .first();
          }
        }
    );
  }

  @Override
  public DateTime lastUpdatedTime()
  {
    return lock.lastUpdatedTime();
  }

  @Override
  public boolean disableSegment(String ds, final String segmentId)
  {
    try {
      connector.getDBI().withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              handle.createStatement(
                  String.format("UPDATE %s SET used=false WHERE id = :segmentID", getSegmentsTable())
              ).bind("segmentID", segmentId)
                    .execute();

              return null;
            }
          }
      );

      DruidDataSource dataSource = getInventoryValue(ds);
      if (dataSource == null) {
        log.warn("Cannot find datasource %s", ds);
        return false;
      }

      dataSource.removeSegment(segmentId);
    }
    catch (Exception e) {
      log.error(e, e.toString());
      return false;
    }

    return true;
  }

  @Override
  public int disableSegments(final String ds, final Interval interval)
  {
    try {
      List<String> disabled = connector.getDBI().withHandle(
          new HandleCallback<List<String>>()
          {
            @Override
            public List<String> withHandle(Handle handle) throws Exception
            {
              final String segmentsTable = getSegmentsTable();
              final String start = interval.getStart().toString();
              final String end = interval.getEnd().toString();
              final Query<Map<String, Object>> query = handle.createQuery(
                  String.format(
                      "SELECT id FROM %s WHERE dataSource = :dataSource and start >= :start and \"end\" <= :end and used=true",
                      segmentsTable
                  )
              );
              List<String> ids = query.bind("dataSource", ds)
                                      .bind("start", start)
                                      .bind("end", end)
                                      .map(
                                          new BaseResultSetMapper<String>()
                                          {
                                            @Override
                                            protected String mapInternal(int index, Map<String, Object> row)
                                            {
                                              return (String) row.get("id");
                                            }
                                          }
                                      )
                                      .list();
              handle.createStatement(
                  String.format(
                      "UPDATE %s SET used=false WHERE dataSource = :dataSource and start >= :start and \"end\" <= :end",
                      segmentsTable
                  ))
                    .bind("dataSource", ds)
                    .bind("start", start)
                    .bind("end", end)
                    .execute();

              return ids;
            }
          }
      );

      DruidDataSource dataSource = getInventoryValue(ds);
      if (dataSource != null) {
        for (String segmentId : disabled) {
          dataSource.removeSegment(segmentId);
        }
      }
      return disabled.size();
    }
    catch (Exception e) {
      log.error(e, e.toString());
      return -1;
    }
  }

  @Override
  public boolean isStarted()
  {
    return lock.isStarted();
  }

  @Override
  public TableDesc getDataSourceDesc(String ds)
  {
    DataSourceMetadata metadata = metaDataCoordinator.getDataSourceMetadata(ds);
    return metadata == null ? null : metadata.getTableDesc();
  }

  @Override
  public DruidDataSource getInventoryValue(String key)
  {
    synchronized (dataSources) {
      return dataSources.get(key);
    }
  }

  @Override
  public ImmutableList<DruidDataSource> getInventory()
  {
    synchronized (dataSources) {
      return ImmutableList.copyOf(dataSources.values());
    }
  }

  @Override
  public ImmutableList<String> getAllDatasourceNames()
  {
    return ImmutableList.copyOf(connector.getDBI().withHandle(
        new HandleCallback<List<String>>()
        {
          @Override
          public List<String> withHandle(Handle handle) throws Exception
          {
            return handle.createQuery(String.format("SELECT DISTINCT(datasource) FROM %s", getSegmentsTable()))
                         .fold(
                             Lists.<String>newArrayList(),
                             new Folder3<List<String>, Map<String, Object>>()
                             {
                               @Override
                               public List<String> fold(
                                   List<String> druidDataSources,
                                   Map<String, Object> stringObjectMap,
                                   FoldController foldController,
                                   StatementContext statementContext
                               ) throws SQLException
                               {
                                 druidDataSources.add(
                                     MapUtils.getString(stringObjectMap, "datasource")
                                 );
                                 return druidDataSources;
                               }
                             }
                         );

          }
        }
    ));
  }

  @Override
  public void poll()
  {
    if (!lock.isStarted()) {
      return;
    }
    try {
      log.debug("Starting polling of segment table");

      // some databases such as PostgreSQL require auto-commit turned off
      // to stream results back, enabling transactions disables auto-commit
      //
      // setting connection to read-only will allow some database such as MySQL
      // to automatically use read-only transaction mode, further optimizing the query
      long start = System.currentTimeMillis();
      int added = syncNewSegments();
      if (added > 0) {
        log.info(
            "Polled and found new %,d segments in the database, took %,d msec",
            added, System.currentTimeMillis() - start
        );
      }
      int deleted = syncDeletedSegments();
      if (deleted > 0) {
        log.info("Removed %,d segments not in the database", deleted);
      }

      final List<String> emptyDS = Lists.newArrayList();
      synchronized (dataSources) {
        for (DruidDataSource dataSource : dataSources.values()) {
          if (dataSource.isEmpty()) {
            emptyDS.add(dataSource.getName());
          }
        }
        for (String empty : emptyDS) {
          dataSources.remove(empty);
        }
      }
      if (!emptyDS.isEmpty()) {
        log.info("Removed empty dataSources %s", emptyDS);
      }
      done();
    }
    catch (Exception e) {
      log.makeAlert(e, "Problem polling DB.").emit();
    }
  }

  // add segments in meta
  private int syncNewSegments() throws SQLException
  {
    final String sql = String.format(
        "SELECT id, dataSource, payload FROM %s WHERE used=true", getSegmentsTable()
    );
    return inReadOnlyTransaction(
        new TransactionCallback<MutableInt>()
        {
          @Override
          public MutableInt inTransaction(Handle handle, TransactionStatus status) throws Exception
          {
            return handle
                .createQuery(sql)
                .setFetchSize(connector.getStreamingFetchSize())
                .fold(new MutableInt(), new Folder2<MutableInt>()
                {
                  @Override
                  public MutableInt fold(MutableInt accumulator, ResultSet r, StatementContext ctx) throws SQLException
                  {
                    if (!dataSources.isEmpty()) {
                      final DruidDataSource dataSource = getInventoryValue(r.getString("dataSource"));
                      if (dataSource != null && dataSource.contains(r.getString("id"))) {
                        return accumulator;
                      }
                    }
                    try {
                      final DataSegment segment = jsonMapper.readValue(r.getBytes("payload"), DataSegment.class);
                      if (registerToView((segment))) {
                        accumulator.increment();
                      }
                    }
                    catch (IOException e) {
                      log.makeAlert(e, "Failed to read segment from db.");
                    }
                    return accumulator;
                  }
                });
          }
        }
    ).intValue();
  }

  // remove segments not in meta
  private int syncDeletedSegments() throws SQLException
  {
    final String counter = String.format(
        "SELECT count(*) FROM %s WHERE dataSource = :dataSource AND used=true",
        getSegmentsTable()
    );
    final String retrieve = String.format(
        "SELECT id FROM %s WHERE dataSource = :dataSource AND used=true",
        getSegmentsTable()
    );
    int count = 0;
    for (DruidDataSource dataSource : getInventory()) {
      for (String delta : inReadOnlyTransaction(
          new TransactionCallback<Set<String>>()
          {
            @Override
            public Set<String> inTransaction(Handle handle, TransactionStatus status) throws Exception
            {
              int count = handle.createQuery(counter)
                                .bind("dataSource", dataSource.getName())
                                .map(IntegerMapper.FIRST)
                                .first();
              if (dataSource.size() > count) {
                final Set<String> toRemove = dataSource.getCopyOfSegmentIds();
                for (String existing : handle.createQuery(retrieve)
                                             .setFetchSize(connector.getStreamingFetchSize())
                                             .bind("dataSource", dataSource.getName())
                                             .map(StringMapper.FIRST)) {
                  toRemove.remove(existing);
                }
                return toRemove;
              }
              return ImmutableSet.of();
            }
          }
      )) {
        if (dataSource.removeSegment(delta)) {
          count++;
        }
      }
    }
    return count;
  }

  private void done()
  {
    synchronized (lock) {
      lock.lastUpdatedTime = DateTimes.nowUtc();
    }
    int count = 0;
    for (DruidDataSource dataSource : getInventory()) {
      count += dataSource.size();
    }
    log.info("%,d segments in heap: %s", count, toString(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage()));
  }

  private static String toString(MemoryUsage mbean)
  {
    return String.format("%,dM/%,dM", mbean.getUsed() >> 20, mbean.getMax() >> 20);
  }

  private String getSegmentsTable()
  {
    return dbTables.get().getSegmentsTable();
  }

  @Override
  public List<Interval> getUnusedSegmentIntervals(
      final String dataSource,
      final Interval interval,
      final int limit
  )
  {
    return inReadOnlyTransaction(
        new TransactionCallback<List<Interval>>()
        {
          @Override
          public List<Interval> inTransaction(Handle handle, TransactionStatus status) throws Exception
          {
            Iterator<Interval> iter = handle
                .createQuery(
                    String.format(
                        "SELECT start, \"end\" FROM %s WHERE dataSource = :dataSource and start >= :start and \"end\" <= :end and used = false ORDER BY start, \"end\"",
                        getSegmentsTable()
                    )
                )
                .setFetchSize(connector.getStreamingFetchSize())
                .bind("dataSource", dataSource)
                .bind("start", interval.getStart().toString())
                .bind("end", interval.getEnd().toString())
                .map(
                    new BaseResultSetMapper<Interval>()
                    {
                      @Override
                      protected Interval mapInternal(int index, Map<String, Object> row)
                      {
                        return new Interval(
                            DateTime.parse((String) row.get("start")),
                            DateTime.parse((String) row.get("end"))
                        );
                      }
                    }
                )
                .iterator();


            List<Interval> result = Lists.newArrayListWithCapacity(limit);
            for (int i = 0; i < limit && iter.hasNext(); i++) {
              try {
                result.add(iter.next());
              }
              catch (Exception e) {
                throw Throwables.propagate(e);
              }
            }
            return result;
          }
        }
    );
  }

  @Override
  public Interval getUmbrellaInterval(final String ds)
  {
    return inReadOnlyTransaction(
        new TransactionCallback<Interval>()
        {
          @Override
          public Interval inTransaction(Handle handle, TransactionStatus status) throws Exception
          {
            return JodaUtils.umbrellaInterval(Iterators.filter(
                handle
                    .createQuery(
                        String.format(
                            "SELECT min(start) as start, max(\"end\") as \"end\" FROM %s WHERE dataSource = :dataSource",
                            getSegmentsTable()
                        )
                    )
                    .bind("dataSource", ds)
                    .map(
                        new BaseResultSetMapper<Interval>()
                        {
                          @Override
                          protected Interval mapInternal(int index, Map<String, Object> row)
                          {
                            final String start = (String) row.get("start");
                            final String end = (String) row.get("end");
                            if (start == null || end == null) {
                              return null;
                            }
                            return new Interval(DateTime.parse(start), DateTime.parse(end));
                          }
                        }
                    )
                    .iterator(),
                Predicates.notNull()
            ));
          }
        }
    );
  }

  @Override
  public String toString()
  {
    return String.format("%s@%s", getClass().getName(), connector.getConfig().getConnectURI());
  }
}
