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
package io.druid.sql.calcite.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.druid.client.ImmutableDruidServer;
import io.druid.client.JsonParserIterator;
import io.druid.client.TimelineServerView;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.client.selector.ServerSelector;
import io.druid.common.guava.HostAndPort;
import io.druid.common.utils.PropUtils;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.indexer.TaskStatusPlus;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.java.util.http.client.Request;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunners;
import io.druid.query.jmx.JMXQuery;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordinator.BytesAccumulatingResponseHandler;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.OperatorKey;
import io.druid.sql.calcite.table.RowSignature;
import io.druid.timeline.DataSegment;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.DefaultEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SystemSchema extends AbstractSchema
{
  private static final Logger log = new Logger(SystemSchema.class);

  public static final String NAME = "sys";
  private static final String SEGMENTS_TABLE = "segments";
  private static final String SERVERS_TABLE = "servers";
  private static final String SERVERS_EXTENDED_TABLE = "servers_extended";
  private static final String SERVER_SEGMENTS_TABLE = "server_segments";
  private static final String TASKS_TABLE = "tasks";
  private static final String LOCKS_TABLE = "locks";
  private static final String FUNCTIONS_TABLE = "functions";

  static final RowSignature SEGMENTS_SIGNATURE = RowSignature
      .builder()
      .add("segment_id", ValueDesc.STRING)
      .add("datasource", ValueDesc.STRING)
      .add("start", ValueDesc.STRING)
      .add("end", ValueDesc.STRING)
      .add("size", ValueDesc.LONG)
      .add("version", ValueDesc.STRING)
      .add("partition_num", ValueDesc.LONG)
      .add("num_replicas", ValueDesc.LONG)
      .add("num_rows", ValueDesc.LONG)
      .add("is_published", ValueDesc.LONG)
      .add("is_available", ValueDesc.LONG)
      .add("is_realtime", ValueDesc.LONG)
      .build();

  static final RowSignature SERVERS_SIGNATURE = RowSignature
      .builder()
      .add("server", ValueDesc.STRING)
      .add("host", ValueDesc.STRING)
      .add("plaintext_port", ValueDesc.LONG)
      .add("tls_port", ValueDesc.LONG)
      .add("server_type", ValueDesc.STRING)
      .add("tier", ValueDesc.STRING)
      .add("curr_size", ValueDesc.LONG)
      .add("max_size", ValueDesc.LONG)
      .build();

  static final RowSignature SERVERS_EXTENDED_SIGNATURE = RowSignature
      .builderFrom(SERVERS_SIGNATURE)
      .add("availableProcessor", ValueDesc.LONG)
      .add("systemLoadAverage", ValueDesc.DOUBLE)
      .add("heap_max", ValueDesc.LONG)
      .add("heap_used", ValueDesc.LONG)
      .add("heap_committed", ValueDesc.LONG)
      .add("non_heap_max", ValueDesc.LONG)
      .add("non_heap_used", ValueDesc.LONG)
      .add("non_heap_committed", ValueDesc.LONG)
      .add("threadCount", ValueDesc.LONG)
      .add("peakThreadCount", ValueDesc.LONG)
      .add("totalStartedThreadCount", ValueDesc.LONG)
      .add("gc_collectionCount", ValueDesc.MAP)
      .add("gc_collectionTime", ValueDesc.MAP)
      .build();

  static final RowSignature SERVER_SEGMENTS_SIGNATURE = RowSignature
      .builder()
      .add("server", ValueDesc.STRING)
      .add("segment_id", ValueDesc.STRING)
      .build();

  static final RowSignature TASKS_SIGNATURE = RowSignature
      .builder()
      .add("task_id", ValueDesc.STRING)
      .add("type", ValueDesc.STRING)
      .add("datasource", ValueDesc.STRING)
      .add("created_time", ValueDesc.STRING)
      .add("queue_insertion_time", ValueDesc.STRING)
      .add("status", ValueDesc.STRING)
      .add("runner_status", ValueDesc.STRING)
      .add("duration", ValueDesc.LONG)
      .add("location", ValueDesc.STRING)
      .add("host", ValueDesc.STRING)
      .add("plaintext_port", ValueDesc.LONG)
      .add("tls_port", ValueDesc.LONG)
      .add("error_msg", ValueDesc.STRING)
      .build();

  static final RowSignature LOCKS_SIGNATURE = RowSignature
      .builder()
      .add("datasource", ValueDesc.STRING)
      .add("version", ValueDesc.STRING)
      .add("interval", ValueDesc.STRING)
      .add("tasks", ValueDesc.ofArray(ValueDesc.STRING))
      .build();

  static final RowSignature FUNCTIONS_SIGNATURE = RowSignature
      .builder()
      .add("name", ValueDesc.STRING)
      .add("type", ValueDesc.STRING)
      .add("external", ValueDesc.BOOLEAN)
      .build();

  private final Map<String, Table> tableMap;

  @Inject
  public SystemSchema(
      final DruidSchema druidSchema,
      final TimelineServerView serverView,
      final CoordinatorClient coordinatorDruidLeaderClient,
      final IndexingServiceClient overlordDruidLeaderClient,
      final DruidOperatorTable operatorTable,
      final ObjectMapper jsonMapper
  )
  {
    Preconditions.checkNotNull(serverView, "serverView");
    BytesAccumulatingResponseHandler responseHandler = new BytesAccumulatingResponseHandler();
    this.tableMap = ImmutableMap.<String, Table>builder()
      .put(
          SEGMENTS_TABLE,
          new SegmentsTable(serverView, jsonMapper)
      )
      .put(
          SERVERS_TABLE,
          new ServersTable(serverView)
      )
      .put(
          SERVERS_EXTENDED_TABLE,
          new ServersExtendedTable(serverView)
      )
      .put(
          SERVER_SEGMENTS_TABLE,
          new ServerSegmentsTable(serverView)
      )
      .put(
          TASKS_TABLE,
          new TasksTable(overlordDruidLeaderClient, jsonMapper, responseHandler)
      )
      .put(
          LOCKS_TABLE,
          new LocksTable(overlordDruidLeaderClient, jsonMapper)
      )
      .put(
          FUNCTIONS_TABLE,
          new FunctionsTable(operatorTable, jsonMapper)
      )
      .build();
  }

  @Override
  public Map<String, Table> getTableMap()
  {
    return tableMap;
  }

  static class SegmentsTable extends AbstractTable implements ScannableTable
  {
    private final TimelineServerView serverView;
    private final ObjectMapper jsonMapper;

    public SegmentsTable(TimelineServerView serverView, ObjectMapper jsonMapper)
    {
      this.serverView = serverView;
      this.jsonMapper = jsonMapper;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return SEGMENTS_SIGNATURE.toRelDataType(typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      Iterable<ServerSelector> selectors = Iterables.concat(Iterables.transform(
          serverView.getDataSources(),
          new Function<String, Iterable<ServerSelector>>()
          {
            @Override
            public Iterable<ServerSelector> apply(String dataSource)
            {
              return serverView.getSelectors(dataSource);
            }
          }
      ));
      Iterable<Object[]> segments = Iterables.transform(selectors, new Function<ServerSelector, Object[]>()
      {
        @Override
        public Object[] apply(ServerSelector input)
        {
          final DataSegment segment = input.getSegment();
          if (segment == null) {
            return null;
          }
          final List<DruidServerMetadata> candidates = input.getCandidates();
          long isRealtime = 0;
          long isPublished = 0;
          for (DruidServerMetadata server : candidates) {
            isRealtime += server.isAssignable() ? 0 : 1;
            isPublished += server.isHistorical() ? 1 : 0;
          }
          return new Object[]{
              segment.getIdentifier(),
              segment.getDataSource(),
              segment.getInterval().getStart().toString(),
              segment.getInterval().getEnd().toString(),
              segment.getSize(),
              segment.getVersion(),
              Long.valueOf(segment.getShardSpecWithDefault().getPartitionNum()),
              candidates.size(),
              segment.getNumRows(),
              isPublished,
              1,
              isRealtime
          };
        }
      });
      return Linq4j.asEnumerable(segments).where(t -> t != null);
    }
  }

  static class ServersTable extends AbstractTable implements ScannableTable
  {
    private final TimelineServerView serverView;

    public ServersTable(TimelineServerView serverView)
    {
      this.serverView = serverView;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return SERVERS_SIGNATURE.toRelDataType(typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      final List<ImmutableDruidServer> druidServers = serverView.getDruidServers();
      final FluentIterable<Object[]> results = FluentIterable
          .from(druidServers)
          .transform(val -> new Object[]{
              val.getHost(),
              extractHost(val.getHost()),
              (long) extractPort(val.getHostAndPort()),
              (long) extractPort(val.getHostAndTlsPort()),
              toStringOrNull(val.getType()),
              val.getTier(),
              val.getCurrSize(),
              val.getMaxSize()
          });
      return Linq4j.asEnumerable(results);
    }
  }

  // todo implement FilterableTable
  static class ServersExtendedTable extends AbstractTable implements ScannableTable
  {
    private final TimelineServerView serverView;

    public ServersExtendedTable(TimelineServerView serverView)
    {
      this.serverView = serverView;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return SERVERS_EXTENDED_SIGNATURE.toRelDataType(typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      final List<ImmutableDruidServer> druidServers = serverView.getDruidServers();
      final FluentIterable<Object[]> results = FluentIterable
          .from(druidServers)
          .transform(val -> {
            final Map<String, Object> stat = getStatFrom(val);
            return new Object[]{
                val.getHost(),
                extractHost(val.getHost()),
                (long) extractPort(val.getHostAndPort()),
                (long) extractPort(val.getHostAndTlsPort()),
                toStringOrNull(val.getType()),
                val.getTier(),
                val.getCurrSize(),
                val.getMaxSize(),
                PropUtils.parseLong(stat, "availableProcessor", -1),
                PropUtils.parseDouble(stat, "systemLoadAverage", -1),
                PropUtils.parseLong(stat, "heap.max", -1),
                PropUtils.parseLong(stat, "heap.used", -1),
                PropUtils.parseLong(stat, "heap.committed", -1),
                PropUtils.parseLong(stat, "non-heap.max", -1),
                PropUtils.parseLong(stat, "non-heap.used", -1),
                PropUtils.parseLong(stat, "non-heap.committed", -1),
                PropUtils.parseLong(stat, "threadCount", -1),
                PropUtils.parseLong(stat, "peakThreadCount", -1),
                PropUtils.parseLong(stat, "totalStartedThreadCount", -1),
                stat.get("gc.collectionCount"),
                stat.get("gc.collectionTime")
            };
          });
      return Linq4j.asEnumerable(results);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getStatFrom(ImmutableDruidServer val)
    {
      JMXQuery query = (JMXQuery) JMXQuery.of(null).withId(UUID.randomUUID().toString());
      QueryRunner<Map<String, Object>> runner = serverView.getQueryRunner(query, val.toDruidServer());
      Map<String, Object> stats = Sequences.only(QueryRunners.run(query, runner), ImmutableMap.of());
      return (Map<String, Object>) Iterables.getOnlyElement(stats.values(), ImmutableMap.of());
    }
  }

  private static class ServerSegmentsTable extends AbstractTable implements ScannableTable
  {
    private final TimelineServerView serverView;

    public ServerSegmentsTable(TimelineServerView serverView)
    {
      this.serverView = serverView;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return SERVER_SEGMENTS_SIGNATURE.toRelDataType(typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      final List<Object[]> rows = new ArrayList<>();
      final List<ImmutableDruidServer> druidServers = serverView.getDruidServers();
      final int serverSegmentsTableSize = SERVER_SEGMENTS_SIGNATURE.size();
      for (ImmutableDruidServer druidServer : druidServers) {
        final Map<String, DataSegment> segmentMap = druidServer.getSegments();
        for (DataSegment segment : segmentMap.values()) {
          Object[] row = new Object[serverSegmentsTableSize];
          row[0] = druidServer.getHost();
          row[1] = segment.getIdentifier();
          rows.add(row);
        }
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  static class TasksTable extends AbstractTable implements ScannableTable
  {
    private final IndexingServiceClient druidLeaderClient;
    private final ObjectMapper jsonMapper;
    private final BytesAccumulatingResponseHandler responseHandler;

    public TasksTable(
        IndexingServiceClient druidLeaderClient,
        ObjectMapper jsonMapper,
        BytesAccumulatingResponseHandler responseHandler
    )
    {
      this.druidLeaderClient = druidLeaderClient;
      this.jsonMapper = jsonMapper;
      this.responseHandler = responseHandler;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return TASKS_SIGNATURE.toRelDataType(typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      class TasksEnumerable extends DefaultEnumerable<Object[]>
      {
        private final CloseableIterator<TaskStatusPlus> it;

        public TasksEnumerable(JsonParserIterator<TaskStatusPlus> tasks)
        {
          this.it = getAuthorizedTasks(tasks, root);
        }

        @Override
        public Iterator<Object[]> iterator()
        {
          return new CloseableIterator<Object[]>()
          {
            private final Enumerator<Object[]> enumerator = enumerator();

            @Override
            public void close() throws IOException
            {
              enumerator.close();
            }

            @Override
            public boolean hasNext()
            {
              return enumerator.moveNext();
            }

            @Override
            public Object[] next()
            {
              return enumerator.current();
            }
          };
        }

        @Override
        public Enumerator<Object[]> enumerator()
        {
          return new Enumerator<Object[]>()
          {
            @Override
            public Object[] current()
            {
              TaskStatusPlus task = it.next();
              // TODO # 4270 tlsPort
              final String hostAndPort;

              if (task.getLocation().getHost() == null) {
                hostAndPort = null;
              } else {
                final int port = task.getLocation().getPort();

                hostAndPort = HostAndPort.fromParts(task.getLocation().getHost(), port).toString();
              }
              return new Object[]{
                  task.getId(),
                  task.getType(),
                  task.getDataSource(),
                  toStringOrNull(task.getCreatedTime()),
                  toStringOrNull(task.getQueueInsertionTime()),
                  toStringOrNull(task.getStatusCode()),
                  toStringOrNull(task.getRunnerStatusCode()),
                  task.getDuration() == null ? 0L : task.getDuration(),
                  hostAndPort,
                  task.getLocation().getHost(),
                  (long) task.getLocation().getPort(),
                  -1L,
                  task.getErrorMsg()
              };
            }

            @Override
            public boolean moveNext()
            {
              return it.hasNext();
            }

            @Override
            public void reset()
            {

            }

            @Override
            public void close()
            {
              try {
                it.close();
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          };
        }
      }

      return new TasksEnumerable(getTasks(druidLeaderClient, jsonMapper, responseHandler));
    }

    private CloseableIterator<TaskStatusPlus> getAuthorizedTasks(
        JsonParserIterator<TaskStatusPlus> it,
        DataContext root
    )
    {
      return wrap(it, it);
    }
  }

  //Note that overlord must be up to get tasks
  private static JsonParserIterator<TaskStatusPlus> getTasks(
      IndexingServiceClient serviceClient,
      ObjectMapper jsonMapper,
      BytesAccumulatingResponseHandler responseHandler
  )
  {
    Request request = serviceClient.makeRequest(HttpMethod.GET, StringUtils.format("/tasks"));
    ListenableFuture<InputStream> future = serviceClient.goAsync(
        request,
        responseHandler
    );
    final JavaType typeRef = jsonMapper.getTypeFactory().constructType(new TypeReference<TaskStatusPlus>() {});
    return new JsonParserIterator<>(jsonMapper, typeRef, request.getUrl(), "", () -> future.get());
  }

  private static <T> CloseableIterator<T> wrap(Iterator<T> iterator, JsonParserIterator<T> it)
  {
    return new CloseableIterator<T>()
    {
      @Override
      public boolean hasNext()
      {
        final boolean hasNext = iterator.hasNext();
        if (!hasNext) {
          try {
            it.close();
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        return hasNext;
      }

      @Override
      public T next()
      {
        return iterator.next();
      }

      @Override
      public void close() throws IOException
      {
        it.close();
      }
    };
  }

  private static final TypeReference<List<Map<String, Object>>> ROWS = new TypeReference<List<Map<String, Object>>>()
  {
  };

  static class LocksTable extends AbstractTable implements ScannableTable
  {
    private final IndexingServiceClient druidLeaderClient;
    private final ObjectMapper jsonMapper;

    public LocksTable(IndexingServiceClient druidLeaderClient, ObjectMapper jsonMapper)
    {
      this.druidLeaderClient = druidLeaderClient;
      this.jsonMapper = jsonMapper;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return LOCKS_SIGNATURE.toRelDataType(typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      List<String> columns = LOCKS_SIGNATURE.getColumnNames();
      List<Object[]> rows = Lists.newArrayList();
      for (Map<String, Object> row : druidLeaderClient.execute(HttpMethod.GET, "locks/_/_", ROWS)) {
        Object[] array = new Object[columns.size()];
        for (int i = 0; i < array.length; i++) {
          array[i] = row.get(columns.get(i));
        }
        rows.add(array);
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  static class FunctionsTable extends AbstractTable implements ScannableTable
  {
    private final DruidOperatorTable operatorTable;
    private final ObjectMapper jsonMapper;

    public FunctionsTable(DruidOperatorTable operatorTable, ObjectMapper jsonMapper)
    {
      this.operatorTable = operatorTable;
      this.jsonMapper = jsonMapper;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return FUNCTIONS_SIGNATURE.toRelDataType(typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      List<Object[]> rows = Lists.newArrayList();
      for (OperatorKey key : operatorTable.getAggregators().keySet()) {
        rows.add(new Object[] {key.getName(), "UDAF", key.isExternal()});
      }
      for (OperatorKey key : operatorTable.getOperatorConversions().keySet()) {
        rows.add(new Object[] {key.getName(), "UDF", key.isExternal()});
      }
      for (OperatorKey key : operatorTable.getDimFilterConversions().keySet()) {
        rows.add(new Object[] {key.getName(), "UDF", key.isExternal()});
      }
      Collections.sort(rows, new Comparator<Object[]>()
      {
        @Override
        public int compare(Object[] o1, Object[] o2)
        {
          return ((String) o1[0]).compareTo((String) o2[0]);
        }
      });
      return Linq4j.asEnumerable(rows);
    }
  }

  @Nullable
  private static String extractHost(@Nullable final String hostAndPort)
  {
    if (hostAndPort == null) {
      return null;
    }

    return HostAndPort.fromString(hostAndPort).getHostText();
  }

  private static int extractPort(@Nullable final String hostAndPort)
  {
    if (hostAndPort == null) {
      return -1;
    }

    return HostAndPort.fromString(hostAndPort).getPortOrDefault(-1);
  }

  @Nullable
  private static String toStringOrNull(@Nullable final Object object)
  {
    if (object == null) {
      return null;
    }

    return object.toString();
  }

}
