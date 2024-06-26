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

package io.druid.query.datasourcemetadata;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.common.guava.BaseSequence;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.ISE;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;

import java.util.Iterator;
import java.util.Map;

/**
 */
public class DataSourceMetadataQueryRunnerFactory
    extends QueryRunnerFactory.Abstract<Result<DataSourceMetadataResultValue>>
{
  @Inject
  public DataSourceMetadataQueryRunnerFactory(
      DataSourceQueryQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    super(toolChest, queryWatcher);
  }

  @Override
  public QueryRunner<Result<DataSourceMetadataResultValue>> _createRunner(
      Segment segment, Supplier<Object> optimizer, SessionCache cache
  )
  {
    return new DataSourceMetadataQueryRunner(segment);
  }

  private static class DataSourceMetadataQueryRunner implements QueryRunner<Result<DataSourceMetadataResultValue>>
  {
    private final StorageAdapter adapter;

    public DataSourceMetadataQueryRunner(Segment segment)
    {
      this.adapter = segment.asStorageAdapter(false);
    }

    @Override
    public Sequence<Result<DataSourceMetadataResultValue>> run(
        Query<Result<DataSourceMetadataResultValue>> input,
        Map<String, Object> responseContext
    )
    {
      if (!(input instanceof DataSourceMetadataQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass().getCanonicalName(), DataSourceMetadataQuery.class);
      }

      final DataSourceMetadataQuery legacyQuery = (DataSourceMetadataQuery) input;

      return Sequences.simple(() -> {
        if (adapter == null) {
          throw new ISE(
              "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
          );
        }

        // ??
        return legacyQuery.buildResult(
            adapter.getInterval().getStart(),
            adapter.getTimeMinMax().getEnd()
        ).iterator();
      });
    }
  }
}
