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

package io.druid.query;

import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MaterializedQuery extends DummyQuery<Object[]>
    implements Query.ArrayOutput, Query.OrderingSupport<Object[]>, Query.SchemaProvider
{
  public static MaterializedQuery of(Query<?> source, List<String> columns, List<Object[]> values)
  {
    return new MaterializedQuery(
        source.getDataSource(),
        null,
        false,
        Sequences.from(columns, values),
        null,
        source.getContext()
    );
  }

  private final List<OrderByColumnSpec> orderingSpecs;
  private RowSignature schema;

  protected MaterializedQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      boolean descending,
      Sequence<Object[]> sequence,
      List<OrderByColumnSpec> orderingSpecs,
      Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, descending, sequence, context);
    this.orderingSpecs = orderingSpecs;
  }

  @Override
  public List<OrderByColumnSpec> getResultOrdering()
  {
    return orderingSpecs;
  }

  @Override
  public MaterializedQuery withResultOrdering(List<OrderByColumnSpec> orderingSpecs)
  {
    return new MaterializedQuery(
        dataSource,
        getQuerySegmentSpec(),
        isDescending(),
        sequence,
        orderingSpecs,
        getContext()
    ).withSchema(schema);
  }

  @Override
  public MaterializedQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new MaterializedQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        sequence,
        orderingSpecs,
        computeOverriddenContext(contextOverride)
    ).withSchema(schema);
  }

  @Override
  public MaterializedQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new MaterializedQuery(
        getDataSource(),
        spec,
        isDescending(),
        sequence,
        orderingSpecs,
        getContext()
    );
  }

  @Override
  public MaterializedQuery withDataSource(DataSource dataSource)
  {
    return new MaterializedQuery(
        dataSource,
        getQuerySegmentSpec(),
        isDescending(),
        sequence,
        orderingSpecs,
        getContext()
    );
  }

  @Override
  public Sequence<Object[]> run(Query<Object[]> query, Map<String, Object> responseContext)
  {
    if (GuavaUtils.isNullOrEmpty(orderingSpecs)) {
      return sequence;
    }
    List<Object[]> values = Sequences.toList(sequence);
    Collections.sort(values, OrderByColumnSpec.toComparator(sequence.columns(), orderingSpecs));
    return Sequences.from(sequence.columns(), values);
  }

  public MaterializedQuery withSchema(RowSignature schema)
  {
    this.schema = schema;
    return this;
  }

  @Override
  public RowSignature schema(QuerySegmentWalker segmentWalker)
  {
    if (schema != null) {
      return schema;
    }
    RowSignature signature = Queries.sourceSchema(this, segmentWalker);
    if (sequence.columns() != null) {
      signature = signature.retain(sequence.columns());
    }
    return signature;
  }

  @Override
  public String toString()
  {
    return "MaterializedQuery{dataSource=" + getDataSource().getNames() + "}";
  }
}
