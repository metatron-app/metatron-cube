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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.druid.client.ImmutableDruidServer;
import io.druid.jackson.JsonParserIterator;
import io.druid.client.TimelineServerView;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.client.selector.ServerSelector;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.HostAndPort;
import io.druid.common.utils.PropUtils;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.indexer.TaskStatusPlus;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.InputStreamResponseHandler;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunners;
import io.druid.query.jmx.JMXQuery;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordinator.BytesAccumulatingResponseHandler;
import io.druid.sql.calcite.Utils;
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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class SystemSchema extends AbstractSchema
{
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
      .add("servers", ValueDesc.STRING_ARRAY)
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
      .add("tasks", ValueDesc.STRING_ARRAY)
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
      final TimelineServerView serverView,
      final CoordinatorClient coordinatorClient,
      final IndexingServiceClient overlordClient,
      final DruidOperatorTable operatorTable,
      final ObjectMapper jsonMapper
  )
  {
    Preconditions.checkNotNull(serverView, "serverView");
    tableMap = ImmutableMap.<String, Table>builder()
        .put(SEGMENTS_TABLE, new SegmentsTable(SEGMENTS_SIGNATURE, serverView))
        .put(SERVERS_TABLE, new ServersTable(SERVERS_SIGNATURE, serverView))
        .put(SERVERS_EXTENDED_TABLE, new ServersExtendedTable(SERVERS_EXTENDED_SIGNATURE, serverView))
        .put(SERVER_SEGMENTS_TABLE, new ServerSegmentsTable(SERVER_SEGMENTS_SIGNATURE, serverView))
        .put(TASKS_TABLE, new TasksTable(TASKS_SIGNATURE, overlordClient, jsonMapper))
        .put(LOCKS_TABLE, new LocksTable(LOCKS_SIGNATURE, overlordClient))
        .put(FUNCTIONS_TABLE, new FunctionsTable(FUNCTIONS_SIGNATURE, operatorTable))
        .build();
  }

  @Override
  public Map<String, Table> getTableMap()
  {
    return tableMap;
  }

  static abstract class SystemTable extends AbstractTable implements ScannableTable
  {
    private final RowSignature signature;

    protected SystemTable(RowSignature signature) {this.signature = signature;}

    @Override
    public final RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return signature.toRelDataType(typeFactory);
    }

    @Override
    public final TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }
  }

  static class SegmentsTable extends SystemTable implements FilterableTable
  {
    private final TimelineServerView serverView;

    public SegmentsTable(RowSignature signature, TimelineServerView serverView)
    {
      super(signature);
      this.serverView = serverView;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      return _scan(serverView.getDataSources(), Arrays.asList());
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters)
    {
      return _scan(filter(serverView.getDataSources(), filters), filters);
    }

    @SuppressWarnings("unchecked")
    private Enumerable<Object[]> _scan(Iterable<String> dataSources, List<RexNode> filters)
    {
      Iterable<ServerSelector> selectors = GuavaUtils.explode(dataSources, ds -> serverView.getSelectors(ds));
      Predicate predicate = Utils.extractFilter(0, filters);   // 0 : SEGMENTS_SIGNATURE.indexOf("segment_id")
      if (predicate != null) {
        selectors = Iterables.filter(selectors, selector -> predicate.apply(selector.getSegment().getIdentifier()));
      }
      return Linq4j.asEnumerable(Iterables.transform(selectors, selector -> toRow(selector)))
                   .where(t -> t != null);
    }

    @SuppressWarnings("unchecked")
    private Iterable<String> filter(Iterable<String> dataSources, List<RexNode> filters)
    {
      Predicate predicate = Utils.extractFilter(1, filters);    // SEGMENTS_SIGNATURE.indexOf("datasource")
      if (predicate != null) {
        dataSources = Iterables.filter(dataSources, predicate);
      }
      return dataSources;
    }

    private Object[] toRow(ServerSelector input)
    {
      final DataSegment segment = input.getSegment();
      if (segment == null) {
        return null;
      }
      final List<DruidServerMetadata> candidates = input.getCandidates();
      long isRealtime = 0;
      long isPublished = 0;
      List<String> servers = Lists.newArrayList();
      for (DruidServerMetadata server : candidates) {
        isRealtime += server.isAssignable() ? 0 : 1;
        isPublished += server.isHistorical() ? 1 : 0;
        servers.add(server.getName() + "(" + server.getType() + ")");
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
          isRealtime,
          servers
      };
    }
  }

  static class ServersTable extends SystemTable implements FilterableTable
  {
    protected final TimelineServerView serverView;

    public ServersTable(RowSignature signature, TimelineServerView serverView)
    {
      super(signature);
      this.serverView = serverView;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      return scan(serverView.getDruidServers());
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters)
    {
      return scan(filter(serverView.getDruidServers(), filters));
    }

    protected Enumerable<Object[]> scan(Iterable<ImmutableDruidServer> servers)
    {
      return Linq4j.asEnumerable(Iterables.transform(
          servers,
          server -> new Object[]{
              server.getHost(),
              extractHost(server.getHost()),
              extractPort(server.getHostAndPort()),
              extractPort(server.getHostAndTlsPort()),
              Objects.toString(server.getType(), null),
              server.getTier(),
              server.getCurrSize(),
              server.getMaxSize()
          }
      ));
    }

    @SuppressWarnings("unchecked")
    protected Iterable<ImmutableDruidServer> filter(Iterable<ImmutableDruidServer> servers, List<RexNode> filters)
    {
      final Predicate serverName = Utils.extractFilter(0, filters);     // 0 : SERVERS.indexOf("server")
      if (serverName != null) {
        servers = Iterables.filter(servers, server -> serverName.apply(server.getName()));
      }
      final Predicate hostName = Utils.extractFilter(1, filters);       // 1 : SERVERS.indexOf("host")
      if (hostName != null) {
        servers = Iterables.filter(servers, server -> hostName.apply(server.getHost()));
      }
      final Predicate serverType = Utils.extractFilter(4, filters);     // 4 : SERVERS.indexOf("server_type")
      if (serverType != null) {
        servers = Iterables.filter(servers, server -> serverType.apply(server.getType()));
      }
      final Predicate tierName = Utils.extractFilter(5, filters);       // 5 : SERVERS.indexOf("tier")
      if (tierName != null) {
        servers = Iterables.filter(servers, server -> tierName.apply(server.getTier()));
      }
      return servers;
    }
  }

  static class ServersExtendedTable extends ServersTable
  {
    public ServersExtendedTable(RowSignature signature, TimelineServerView serverView)
    {
      super(signature, serverView);
    }

    @Override
    protected Enumerable<Object[]> scan(Iterable<ImmutableDruidServer> servers)
    {
      return Linq4j.asEnumerable(Iterables.transform(
          servers, server -> {
            final Map<String, Object> stat = getStatFrom(server);
            return new Object[]{
                server.getHost(),
                extractHost(server.getHost()),
                extractPort(server.getHostAndPort()),
                extractPort(server.getHostAndTlsPort()),
                Objects.toString(server.getType(), null),
                server.getTier(),
                server.getCurrSize(),
                server.getMaxSize(),
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
          }
      ));
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

  private static class ServerSegmentsTable extends SystemTable implements FilterableTable
  {
    private final TimelineServerView serverView;

    public ServerSegmentsTable(RowSignature signature, TimelineServerView serverView)
    {
      super(signature);
      this.serverView = serverView;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      return _scan(serverView.getDruidServers(), Arrays.asList());
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters)
    {
      return _scan(filter(serverView.getDruidServers(), filters), filters);
    }

    @SuppressWarnings("unchecked")
    private Enumerable<Object[]> _scan(Iterable<ImmutableDruidServer> servers, List<RexNode> filters)
    {
      Predicate predicate = Utils.extractFilter(1, filters);   // 1 : SERVER_SEGMENTS.indexOf("segment_id")
      if (predicate != null) {
        return Linq4j.asEnumerable(GuavaUtils.explode(servers, server ->
            Iterables.transform(
                Iterables.filter(server.getSegments().values(), segment -> predicate.apply(server)),
                segment -> new Object[]{server.getHost(), segment.getIdentifier()}
            )
        ));
      }
      return Linq4j.asEnumerable(GuavaUtils.explode(servers, server ->
          Iterables.transform(
              server.getSegments().values(), segment -> new Object[]{server.getHost(), segment.getIdentifier()}
          )
      ));
    }

    @SuppressWarnings("unchecked")
    private Iterable<ImmutableDruidServer> filter(Iterable<ImmutableDruidServer> servers, List<RexNode> filters)
    {
      Predicate predicate = Utils.extractFilter(0, filters);   // 0 : SERVER_SEGMENTS.indexOf("server")
      if (predicate != null) {
        servers = Iterables.filter(servers, server -> predicate.apply(server.getName()));
      }
      return servers;
    }
  }

  static class TasksTable extends SystemTable
  {
    private final IndexingServiceClient overlordClient;
    private final ObjectMapper jsonMapper;
    private final InputStreamResponseHandler responseHandler;

    public TasksTable(
        RowSignature signature,
        IndexingServiceClient overlordClient,
        InputStreamResponseHandler responseHandler,
        ObjectMapper jsonMapper
    )
    {
      super(signature);
      this.overlordClient = overlordClient;
      this.responseHandler = responseHandler;
      this.jsonMapper = jsonMapper;
    }

    public TasksTable(RowSignature tasksSignature, IndexingServiceClient overlordClient, ObjectMapper jsonMapper)
    {
      this(tasksSignature, overlordClient, new BytesAccumulatingResponseHandler(), jsonMapper);
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
                  Objects.toString(task.getCreatedTime(), null),
                  Objects.toString(task.getQueueInsertionTime(), null),
                  Objects.toString(task.getStatusCode(), null),
                  Objects.toString(task.getRunnerStatusCode(), null),
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

      return new TasksEnumerable(getTasks(overlordClient, jsonMapper, responseHandler));
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
      InputStreamResponseHandler responseHandler
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

  static class LocksTable extends SystemTable
  {
    private final IndexingServiceClient overlordClient;

    public LocksTable(RowSignature signature, IndexingServiceClient overlordClient)
    {
      super(signature);
      this.overlordClient = overlordClient;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      List<String> columns = LOCKS_SIGNATURE.getColumnNames();
      List<Object[]> rows = Lists.newArrayList();
      for (Map<String, Object> row : overlordClient.execute(HttpMethod.GET, "locks/_/_", ROWS)) {
        Object[] array = new Object[columns.size()];
        for (int i = 0; i < array.length; i++) {
          array[i] = row.get(columns.get(i));
        }
        rows.add(array);
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  static class FunctionsTable extends SystemTable
  {
    private final DruidOperatorTable operatorTable;

    public FunctionsTable(RowSignature signature, DruidOperatorTable operatorTable)
    {
      super(signature);
      this.operatorTable = operatorTable;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      List<Object[]> rows = Lists.newArrayList();
      for (OperatorKey key : operatorTable.getAggregators().keySet()) {
        rows.add(new Object[]{key.getName(), "UDAF", key.isExternal()});
      }
      for (OperatorKey key : operatorTable.getOperatorConversions().keySet()) {
        rows.add(new Object[]{key.getName(), "UDF", key.isExternal()});
      }
      for (OperatorKey key : operatorTable.getDimFilterConversions().keySet()) {
        rows.add(new Object[]{key.getName(), "UDF", key.isExternal()});
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
    return hostAndPort == null ? null : HostAndPort.fromString(hostAndPort).getHostText();
  }

  private static long extractPort(@Nullable final String hostAndPort)
  {
    return hostAndPort == null ? -1 : HostAndPort.fromString(hostAndPort).getPortOrDefault(-1);
  }
}
