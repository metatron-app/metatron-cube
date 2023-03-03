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

package io.druid.query.ddl;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.segment.IndexIO;
import io.druid.segment.Segment;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.SegmentLoader;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.util.Map;

/**
 *
 */
public class DeleteColumnsFactory extends QueryRunnerFactory.Abstract<DDLResult>
{
  private static final Logger LOG = new Logger(DeleteColumnsFactory.class);

  private final SegmentLoader loader;
  private final IndexIO indexIO;
  private final DataSegmentPusher pusher;

  @Inject
  public DeleteColumnsFactory(
      DeleteColumnsToolChest toolChest,
      SegmentLoader loader,
      IndexIO indexIO,
      DataSegmentPusher pusher,
      QueryWatcher queryWatcher
  )
  {
    super(toolChest, queryWatcher);
    this.loader = Preconditions.checkNotNull(loader, "loader");
    this.indexIO = Preconditions.checkNotNull(indexIO, "indexIO");
    this.pusher = Preconditions.checkNotNull(pusher, "pusher");
  }

  @Override
  public QueryRunner<DDLResult> _createRunner(Segment segment, Supplier<Object> optimizer, SessionCache cache)
  {
    return new QueryRunner<DDLResult>()
    {
      @Override
      public Sequence<DDLResult> run(Query<DDLResult> query, Map<String, Object> responseContext)
      {
        DeleteColumnsQuery delete = (DeleteColumnsQuery) query;
        DataSegment descriptor = segment.getDescriptor();
        File location = loader.getSegmentLocation(descriptor);
        try {
          IndexIO.IndexLoader loader = indexIO.getIndexLoader(location);
          File deleted = loader.deleteColumns(location, null, delete.getColumns().toArray(new String[0]));
          if (deleted != null) {
            DataSegment pushed = pusher.push(deleted, delete.rewrite(descriptor));
            LOG.info("New segment %s pushed to %s", pushed.getIdentifier(), pushed.getLoadSpec());
            return Sequences.simple(DDLResult.success(pushed));
          }
          return Sequences.empty();
        }
        catch (Exception e) {
          return Sequences.simple(DDLResult.fail());
        }
      }
    };
  }
}
