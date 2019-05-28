/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.CloseableIterator;
import com.metamx.http.client.Request;
import io.druid.client.ImmutableDruidServer;
import io.druid.client.JsonParserIterator;
import io.druid.client.TimelineServerView;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.indexer.TaskStatusPlus;
import io.druid.server.coordinator.BytesAccumulatingResponseHandler;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class SystemSchema extends AbstractSchema
{
  private static final Logger log = new Logger(SystemSchema.class);

  public static final String NAME = "sys";
  private static final String SEGMENTS_TABLE = "segments";
  private static final String SERVERS_TABLE = "servers";
  private static final String SERVER_SEGMENTS_TABLE = "server_segments";
  private static final String TASKS_TABLE = "tasks";

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
      .add("payload", ValueDesc.STRING)
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

  private final Map<String, Table> tableMap;

  @Inject
  public SystemSchema(
      final DruidSchema druidSchema,
      final TimelineServerView serverView,
      final CoordinatorClient coordinatorDruidLeaderClient,
      final IndexingServiceClient overlordDruidLeaderClient,
      final ObjectMapper jsonMapper
  )
  {
    Preconditions.checkNotNull(serverView, "serverView");
    BytesAccumulatingResponseHandler responseHandler = new BytesAccumulatingResponseHandler();
    this.tableMap = ImmutableMap.<String, Table>builder()
      .put(
          SEGMENTS_TABLE,
          new SegmentsTable(druidSchema, coordinatorDruidLeaderClient, jsonMapper, responseHandler)
      )
      .put(
          SERVERS_TABLE,
          new ServersTable(serverView)
      )
      .put(
          SERVER_SEGMENTS_TABLE,
          new ServerSegmentsTable(serverView)
      )
      .put(
          TASKS_TABLE,
          new TasksTable(overlordDruidLeaderClient, jsonMapper, responseHandler)
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
    private final DruidSchema druidSchema;
    private final CoordinatorClient druidLeaderClient;
    private final ObjectMapper jsonMapper;
    private final BytesAccumulatingResponseHandler responseHandler;

    public SegmentsTable(
        DruidSchema druidSchemna,
        CoordinatorClient druidLeaderClient,
        ObjectMapper jsonMapper,
        BytesAccumulatingResponseHandler responseHandler
    )
    {
      this.druidSchema = druidSchemna;
      this.druidLeaderClient = druidLeaderClient;
      this.jsonMapper = jsonMapper;
      this.responseHandler = responseHandler;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return SEGMENTS_SIGNATURE.getRelDataType(typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      //get available segments from druidSchema
      final Map<DataSegment, SegmentMetadataHolder> availableSegmentMetadata = druidSchema.getSegmentMetadata();
      final Iterator<Entry<DataSegment, SegmentMetadataHolder>> availableSegmentEntries = availableSegmentMetadata.entrySet()
                                                                                                                  .iterator();

      //get published segments from coordinator
      final JsonParserIterator<DataSegment> metadataSegments = getMetadataSegments(
          druidLeaderClient,
          jsonMapper,
          responseHandler
      );

      Set<String> availableSegmentIds = new HashSet<>();
      //auth check for available segments
      final Iterator<Entry<DataSegment, SegmentMetadataHolder>> authorizedAvailableSegments = getAuthorizedAvailableSegments(
          availableSegmentEntries,
          root
      );

      final FluentIterable<Object[]> availableSegments = FluentIterable
          .from(() -> authorizedAvailableSegments)
          .transform(val -> {
            try {
              if (!availableSegmentIds.contains(val.getKey().getIdentifier())) {
                availableSegmentIds.add(val.getKey().getIdentifier());
              }
              return new Object[]{
                  val.getKey().getIdentifier(),
                  val.getKey().getDataSource(),
                  val.getKey().getInterval().getStart().toString(),
                  val.getKey().getInterval().getEnd().toString(),
                  val.getKey().getSize(),
                  val.getKey().getVersion(),
                  Long.valueOf(val.getKey().getShardSpecWithDefault().getPartitionNum()),
                  val.getValue().getNumReplicas(),
                  val.getValue().getNumRows(),
                  val.getValue().isPublished(), // 1L, //is_published is true for published segments
                  val.getValue().isAvailable(),
                  val.getValue().isRealtime(),
                  jsonMapper.writeValueAsString(val.getKey())
              };
            }
            catch (JsonProcessingException e) {
              throw new RuntimeException(StringUtils.format(
                  "Error getting segment payload for segment %s",
                  val.getKey().getIdentifier()
              ), e);
            }
          });

      //auth check for published segments
      final CloseableIterator<DataSegment> authorizedPublishedSegments = getAuthorizedPublishedSegments(
          metadataSegments,
          root
      );
      final FluentIterable<Object[]> publishedSegments = FluentIterable
          .from(() -> authorizedPublishedSegments)
          .transform(val -> {
            try {
              if (availableSegmentIds.contains(val.getIdentifier())) {
                return null;
              }
              return new Object[]{
                  val.getIdentifier(),
                  val.getDataSource(),
                  val.getInterval().getStart().toString(),
                  val.getInterval().getEnd().toString(),
                  val.getSize(),
                  val.getVersion(),
                  Long.valueOf(val.getShardSpecWithDefault().getPartitionNum()),
                  0L,
                  -1L,
                  1L,
                  0L,
                  0L, // TODO numReplicas
                  jsonMapper.writeValueAsString(val)
              };
            }
            catch (JsonProcessingException e) {
              throw new RuntimeException(StringUtils.format(
                  "Error getting segment payload for segment %s",
                  val.getIdentifier()
              ), e);
            }
          });

      final Iterable<Object[]> allSegments = Iterables.unmodifiableIterable(
          Iterables.concat(availableSegments, publishedSegments));

      return Linq4j.asEnumerable(allSegments).where(t -> t != null);

    }

    private Iterator<Entry<DataSegment, SegmentMetadataHolder>> getAuthorizedAvailableSegments(
        Iterator<Entry<DataSegment, SegmentMetadataHolder>> availableSegmentEntries,
        DataContext root
    )
    {
      return availableSegmentEntries;
    }
    private CloseableIterator<DataSegment> getAuthorizedPublishedSegments(
        JsonParserIterator<DataSegment> it,
        DataContext root
    )
    {
      return wrap(it, it);
    }
  }

  // Note that coordinator must be up to get segments
  private static JsonParserIterator<DataSegment> getMetadataSegments(
      CoordinatorClient coordinatorClient,
      ObjectMapper jsonMapper,
      BytesAccumulatingResponseHandler responseHandler
  )
  {

    Request request;
    try {
      request = coordinatorClient.makeRequest(
          HttpMethod.GET,
          StringUtils.format("/metadata/segments")
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    ListenableFuture<InputStream> future = coordinatorClient.goAsync(
        request,
        responseHandler
    );
    final JavaType typeRef = jsonMapper.getTypeFactory().constructType(new TypeReference<DataSegment>()
    {
    });
    return new JsonParserIterator.FromFutureStream<>(
        jsonMapper,
        typeRef,
        request.getUrl(),
        "",
        future,
        -1
    );
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
      return SERVERS_SIGNATURE.getRelDataType(typeFactory);
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
      return SERVER_SEGMENTS_SIGNATURE.getRelDataType(typeFactory);
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
      final int serverSegmentsTableSize = SERVER_SEGMENTS_SIGNATURE.getRowOrder().size();
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
      return TASKS_SIGNATURE.getRelDataType(typeFactory);
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
          throw new UnsupportedOperationException("Do not use iterator(), it cannot be closed.");
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

    private CloseableIterator<TaskStatusPlus> getAuthorizedTasks(JsonParserIterator<TaskStatusPlus> it, DataContext root)
    {
      return wrap(it, it);
    }

  }

  //Note that overlord must be up to get tasks
  private static JsonParserIterator<TaskStatusPlus> getTasks(
      IndexingServiceClient indexingServiceClient,
      ObjectMapper jsonMapper,
      BytesAccumulatingResponseHandler responseHandler
  )
  {

    Request request;
    try {
      request = indexingServiceClient.makeRequest(
          HttpMethod.GET,
          StringUtils.format("/tasks")
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    ListenableFuture<InputStream> future = indexingServiceClient.goAsync(
        request,
        responseHandler
    );
    final JavaType typeRef = jsonMapper.getTypeFactory().constructType(new TypeReference<TaskStatusPlus>()
    {
    });
    return new JsonParserIterator.FromFutureStream<>(
        jsonMapper,
        typeRef,
        request.getUrl(),
        "",
        future,
        -1
    );
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
