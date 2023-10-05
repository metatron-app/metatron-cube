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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Preconditions;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.BulkRow;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.SemiJoinFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("broadcastJoin")
public class BroadcastJoinProcessor extends CommonJoinProcessor
    implements PostProcessingOperator.LogProvider, RowSignature.Evolving
{
  private final ObjectMapper mapper;
  private final QueryConfig config;
  private final JoinElement element;
  private final boolean hashLeft;
  private final RowSignature hashSignature;
  private final byte[] values;
  private final boolean applyFilter;

  @JsonCreator
  public BroadcastJoinProcessor(
      @JacksonInject ObjectMapper mapper,
      @JacksonInject QueryConfig config,
      @JsonProperty("element") JoinElement element,
      @JsonProperty("hashLeft") boolean hashLeft,
      @JsonProperty("hashSignature") RowSignature hashSignature,
      @JsonProperty("prefixAlias") boolean prefixAlias,
      @JsonProperty("asMap") boolean asMap,
      @JsonProperty("outputAlias") List<String> outputAlias,
      @JsonProperty("outputColumns") List<String> outputColumns,
      @JsonProperty("maxOutputRow") int maxOutputRow,
      @JsonProperty("values") byte[] values,
      @JsonProperty("applyFilter") boolean applyFilter
  )
  {
    super(config.getJoin(), prefixAlias, asMap, outputAlias, outputColumns, maxOutputRow);
    this.mapper = mapper;
    this.config = config;
    this.element = element;
    this.hashLeft = hashLeft;
    this.hashSignature = hashSignature;
    this.values = values;
    this.applyFilter = applyFilter;
  }

  @Override
  public BroadcastJoinProcessor withAsMap(boolean asMap)
  {
    return new BroadcastJoinProcessor(
        mapper,
        config,
        element,
        hashLeft,
        hashSignature,
        prefixAlias,
        asMap,
        outputAlias,
        outputColumns,
        maxOutputRow,
        values,
        applyFilter
    );
  }

  @Override
  public CommonJoinProcessor withOutputColumns(List<String> outputColumns)
  {
    return new BroadcastJoinProcessor(
        mapper,
        config,
        element,
        hashLeft,
        hashSignature,
        prefixAlias,
        asMap,
        outputAlias,
        outputColumns,
        maxOutputRow,
        values,
        applyFilter
    );
  }

  @JsonProperty
  public boolean isHashLeft()
  {
    return hashLeft;
  }

  @JsonProperty
  public JoinElement getElement()
  {
    return element;
  }

  @JsonProperty
  public RowSignature getHashSignature()
  {
    return hashSignature;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public byte[] getValues()
  {
    return values;
  }

  @JsonProperty
  public boolean applyFilter()
  {
    return applyFilter;
  }

  @Override
  public QueryRunner postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map response)
      {
        // only works with stream query, for now
        Preconditions.checkArgument(query instanceof Query.ArrayOutputSupport, "?? %s", query);

        List<String> leftAlias = Arrays.asList(element.getLeftAlias());
        List<String> rightAlias = Arrays.asList(element.getRightAlias());
        List<String> leftJoinColumns = element.getLeftJoinColumns();
        List<String> rightJoinColumns = element.getRightJoinColumns();

        LOG.info("Preparing broadcast join %s + %s", leftAlias, rightAlias);

        boolean stringAsRaw = config.useUTF8(query);
        Sequence<BulkRow> rows = Sequences.simple(hashSignature.getColumnNames(), deserializeValue(mapper, values));
        Sequence<Object[]> hashing = Sequences.explode(rows, bulk -> Sequences.once(bulk.decompose(stringAsRaw)));

        if (applyFilter) {
          // todo: stick to bytes if possible
          List<Object[]> materialized = Sequences.toList(hashing);
          List<String> filterOn = hashLeft ? rightJoinColumns : leftJoinColumns;
          int[] indices = GuavaUtils.indexOf(rows.columns(), hashLeft ? leftJoinColumns : rightJoinColumns);
          query = DimFilters.and(query, SemiJoinFactory.from(filterOn, materialized.iterator(), indices));
          hashing = Sequences.from(hashing.columns(), materialized);
          if (hashLeft) {
            LOG.info("-- %s:%d (L) is applied as filter to %s (R)", leftAlias, materialized.size(), rightAlias);
          } else {
            LOG.info("-- %s:%d (R) is applied as filter to %s (L)", rightAlias, materialized.size(), leftAlias);
          }
        }

        Sequence<Object[]> queried = QueryRunners.runArray(query, baseRunner);

        List<String> hashColumns = hashing.columns();
        List<String> queryColumns = queried.columns();

        List<List<String>> names = hashLeft ? Arrays.asList(hashColumns, queryColumns) : Arrays.asList(queryColumns, hashColumns);

        Iterator<Object[]> iterator = Sequences.toIterator(queried);
        if (!iterator.hasNext()) {
          LOG.info("-- driving alias %s (%s) is empty", hashLeft ? rightAlias : leftAlias, hashLeft ? "R" : "L");
          if (outputColumns != null) {
            return Sequences.empty(outputColumns);
          }
          List<String> outputAlias = getOutputAlias();
          if (outputAlias == null) {
            outputAlias = concatColumnNames(names, prefixAlias ? element.getAliases() : null);
          }
          return Sequences.empty(outputAlias);
        }

        JoinAlias left;
        JoinAlias right;
        if (hashLeft) {
          left = toHashAlias(leftAlias, hashColumns, leftJoinColumns, hashing);
          right = toAlias(rightAlias, queryColumns, rightJoinColumns, query, iterator);
        } else {
          left = toAlias(leftAlias, queryColumns, leftJoinColumns, query, iterator);
          right = toHashAlias(rightAlias, hashColumns, rightJoinColumns, hashing);
        }

        List<String> outputAlias = getOutputAlias();
        if (outputAlias == null) {
          outputAlias = concatColumnNames(names, prefixAlias ? element.getAliases() : null);
        }
        int[] projection = projection(outputAlias);
        List<String> projectedNames = outputColumns != null ? outputColumns : GuavaUtils.map(outputAlias, projection);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Running join processing %s resulting %s", toAliases(element), projectedNames);
        }

        JoinResult join = join(element.getJoinType(), left, right, projection);
        return JoinProcessor.format(join.iterator, projectedNames, asMap, query instanceof BaseAggregationQuery);
      }

      private JoinAlias toHashAlias(
          List<String> alias,
          List<String> columns,
          List<String> joinColumns,
          Sequence<Object[]> hashing
      )
      {
        return new JoinAlias(
            alias,
            columns,
            joinColumns,
            GuavaUtils.indexOf(columns, joinColumns, true),
            Sequences.toIterator(hashing)
        );
      }

      private JoinAlias toAlias(
          List<String> alias,
          List<String> columns,
          List<String> joinColumns,
          Query<?> query,
          Iterator<Object[]> queried
      )
      {
        return new JoinAlias(
            alias,
            columns,
            joinColumns,
            getCollations(query),
            GuavaUtils.indexOf(columns, joinColumns, true),
            queried,
            -1
        );
      }
    };
  }

  private static List<BulkRow> deserializeValue(ObjectMapper mapper, byte[] values)
  {
    TypeFactory factory = mapper.getTypeFactory();
    try {
      return mapper.readValue(values, factory.constructParametricType(List.class, BulkRow.class));
    }
    catch (Exception e) {
      throw QueryException.wrapIfNeeded(e);
    }
  }

  @Override
  public List<String> evolve(List<String> schema)
  {
    if (outputColumns != null) {
      return outputColumns;
    }
    if (hashLeft) {
      return GuavaUtils.concat(hashSignature.getColumnNames(), schema);
    } else {
      return GuavaUtils.concat(schema, hashSignature.getColumnNames());
    }
  }

  @Override
  public RowSignature evolve(RowSignature schema)
  {
    RowSignature signature = hashLeft ? hashSignature.concat(schema) : schema.concat(hashSignature);
    if (outputColumns != null) {
      signature = signature.retain(outputColumns);
    }
    return signature;
  }

  @Override
  public PostProcessingOperator forLog()
  {
    return new BroadcastJoinProcessor(
        mapper,
        config,
        element,
        hashLeft,
        hashSignature,
        prefixAlias,
        asMap,
        outputAlias,
        outputColumns,
        maxOutputRow,
        null,
        applyFilter
    );
  }

  @Override
  public String toString()
  {
    return "BroadcastJoinProcessor{" +
           "element=" + element +
           ", hashLeft=" + hashLeft +
           ", hashSignature=" + hashSignature +
           (applyFilter ? ", applyFilter=true" : "") +
           '}';
  }
}
