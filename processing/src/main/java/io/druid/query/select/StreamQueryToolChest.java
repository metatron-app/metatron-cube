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

package io.druid.query.select;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.TabularFormat;
import io.druid.segment.Cursor;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Map;

/**
 */
public class StreamQueryToolChest extends QueryToolChest<StreamQueryRow, StreamQuery>
{
  private static final TypeReference<StreamQueryRow> TYPE_REFERENCE =
      new TypeReference<StreamQueryRow>()
      {
      };

  @Override
  public QueryRunner<StreamQueryRow> mergeResults(final QueryRunner<StreamQueryRow> queryRunner)
  {
    return new QueryRunner<StreamQueryRow>()
    {
      @Override
      public Sequence<StreamQueryRow> run(
          Query<StreamQueryRow> query, Map<String, Object> responseContext
      )
      {
        Sequence<StreamQueryRow> sequence = queryRunner.run(query, responseContext);
        if (((StreamQuery)query).getLimit() > 0) {
          sequence = Sequences.limit(sequence, ((StreamQuery)query).getLimit());
        }
        return sequence;
      }
    };
  }

  @Override
  public TypeReference<StreamQueryRow> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public TabularFormat toTabularFormat(
      final StreamQuery query,
      final Sequence<StreamQueryRow> sequence,
      final String timestampColumn
  )
  {
    return new TabularFormat()
    {
      @Override
      public Sequence<Map<String, Object>> getSequence()
      {
        return Sequences.map(sequence, GuavaUtils.<StreamQueryRow, Map<String, Object>>caster());
      }

      @Override
      public Map<String, Object> getMetaData()
      {
        return null;
      }
    };
  }

  @Override
  public <I> QueryRunner<StreamQueryRow> handleSubQuery(QuerySegmentWalker segmentWalker, int maxRowCount)
  {
    return new StreamingSubQueryRunner<I>(segmentWalker, maxRowCount)
    {
      @Override
      protected final Function<Cursor, Sequence<StreamQueryRow>> streamQuery(
          Query<StreamQueryRow> outerQuery,
          Cursor cursor
      )
      {
        final StreamQuery query = (StreamQuery) outerQuery;
        final String[] columns = query.getColumns().toArray(new String[0]);
        final Function<Cursor, Sequence<Object[]>> converter = StreamQueryEngine.converter(query, new MutableInt());
        return new Function<Cursor, Sequence<StreamQueryRow>>()
        {
          @Override
          public Sequence<StreamQueryRow> apply(Cursor input)
          {
            return Sequences.map(
                converter.apply(input), new Function<Object[], StreamQueryRow>()
                {
                  @Override
                  public StreamQueryRow apply(Object[] input)
                  {
                    final StreamQueryRow row = new StreamQueryRow();
                    for (int i = 0; i < columns.length; i++) {
                      row.put(columns[i], input[i]);
                    }
                    return row;
                  }
                }
            );
          }
        };
      }
    };
  }
}
