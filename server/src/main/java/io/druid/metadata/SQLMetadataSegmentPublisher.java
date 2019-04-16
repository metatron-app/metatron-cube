

/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.NoopEmitter;
import io.druid.server.log.Events;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

public class SQLMetadataSegmentPublisher implements MetadataSegmentPublisher
{
  private static final Logger log = new Logger(SQLMetadataSegmentPublisher.class);

  private final ObjectMapper jsonMapper;
  private final MetadataStorageTablesConfig config;
  private final SQLMetadataConnector connector;
  private final String statement;

  private final Emitter emitter;

  @Inject
  public SQLMetadataSegmentPublisher(
      ObjectMapper jsonMapper,
      MetadataStorageTablesConfig config,
      SQLMetadataConnector connector,
      @Events Emitter emitter
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.connector = connector;
    this.statement = String.format(
        "INSERT INTO %s (id, dataSource, created_date, start, \"end\", partitioned, version, used, payload) "
        + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
        config.getSegmentsTable()
    );
    this.emitter = emitter;
  }

  public SQLMetadataSegmentPublisher(
      ObjectMapper jsonMapper,
      MetadataStorageTablesConfig config,
      SQLMetadataConnector connector
  )
  {
    this(jsonMapper, config, connector, new NoopEmitter());
  }

  private URL toURL(String url)
  {
    try {
      return url == null ? null : new URL(url);
    }
    catch (MalformedURLException e) {
      log.warn(e, "invalid post url %s", url);
      return null;
    }
  }

  @Override
  public void publishSegment(final DataSegment segment) throws IOException
  {
    publishSegment(
        segment.getIdentifier(),
        segment.getDataSource(),
        new DateTime(),
        segment.getInterval().getStart().toString(),
        segment.getInterval().getEnd().toString(),
        (segment.getShardSpecWithDefault() instanceof NoneShardSpec) ? false : true,
        segment.getVersion(),
        true,
        segment
    );
  }

  @VisibleForTesting
  void publishSegment(
      final String identifier,
      final String dataSource,
      final DateTime createdDate,
      final String start,
      final String end,
      final boolean partitioned,
      final String version,
      final boolean used,
      final DataSegment segment
  )
  {
    try {
      final DBI dbi = connector.getDBI();
      List<Map<String, Object>> exists = dbi.withHandle(
          new HandleCallback<List<Map<String, Object>>>()
          {
            @Override
            public List<Map<String, Object>> withHandle(Handle handle) throws Exception
            {
              return handle.createQuery(
                  String.format("SELECT id FROM %s WHERE id=:id", config.getSegmentsTable())
              )
                           .bind("id", identifier)
                           .list();
            }
          }
      );

      if (!exists.isEmpty()) {
        log.info("Found [%s] in DB, not updating DB", identifier);
        return;
      }

      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              handle.createStatement(statement)
                    .bind("id", identifier)
                    .bind("dataSource", dataSource)
                    .bind("created_date", createdDate.toString())
                    .bind("start", start)
                    .bind("end", end)
                    .bind("partitioned", partitioned)
                    .bind("version", version)
                    .bind("used", used)
                    .bind("payload", jsonMapper.writeValueAsBytes(segment))
                    .execute();

              return null;
            }
          }
      );
    }
    catch (Exception e) {
      log.error(e, "Exception inserting into DB");
      throw new RuntimeException(e);
    }
    emitter.emit(
        new Events.SimpleEvent(
            ImmutableMap.<String, Object>of(
                "feed", "SQLMetadataSegmentPublisher",
                "type", "segmentAnnounced",
                "createdDate", System.currentTimeMillis(),
                "payload", segment
            )
        )
    );
  }
}
