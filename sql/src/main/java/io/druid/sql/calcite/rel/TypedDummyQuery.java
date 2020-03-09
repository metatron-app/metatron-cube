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

package io.druid.sql.calcite.rel;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.DataSource;
import io.druid.query.DummyQuery;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.TableDataSource;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.sql.calcite.table.RowSignature;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TypedDummyQuery extends DummyQuery<Object[]>
    implements Query.ArrayOutputSupport<Object[]>, Query.RowOutputSupport<Object[]>, Query.SchemaProvider
{
  public static final TypedDummyQuery DUMMY = of(null, Arrays.<Object[]>asList());

  public static TypedDummyQuery of(RowSignature signature, Iterable<Object[]> sequence)
  {
    return new TypedDummyQuery(
        TableDataSource.of("<NOT-EXISTING>"),
        null,
        false,
        signature,
        Sequences.simple(sequence),
        Maps.<String, Object>newHashMap()
    );
  }

  private final RowSignature signature;

  private TypedDummyQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      boolean descending,
      RowSignature signature,
      Sequence<Object[]> sequence,
      Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, descending, sequence, context);
    this.signature = signature;
  }

  @Override
  public TypedDummyQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new TypedDummyQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        signature,
        sequence,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public TypedDummyQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new TypedDummyQuery(
        getDataSource(),
        spec,
        isDescending(),
        signature,
        sequence,
        getContext()
    );
  }

  @Override
  public TypedDummyQuery withDataSource(DataSource dataSource)
  {
    return new TypedDummyQuery(
        dataSource,
        getQuerySegmentSpec(),
        isDescending(),
        signature,
        sequence,
        getContext()
    );
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    return signature.getColumnNames();
  }

  @Override
  public Sequence<Object[]> array(Sequence<Object[]> sequence)
  {
    return sequence;
  }

  @Override
  public Sequence<Row> asRow(Sequence<Object[]> sequence)
  {
    return Sequences.map(sequence, new Function<Object[], Row>()
    {
      final List<String> columnNames = signature.getColumnNames();

      @Override
      public Row apply(Object[] input)
      {
        final Map<String, Object> converted = Maps.newHashMap();
        for (int i = 0; i < columnNames.size(); i++) {
          converted.put(columnNames.get(i), input[i]);
        }
        return new MapBasedRow(0, converted);
      }
    });
  }

  @Override
  public io.druid.query.RowSignature schema(QuerySegmentWalker segmentWalker)
  {
    return signature;
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof TypedDummyQuery;
  }
}
