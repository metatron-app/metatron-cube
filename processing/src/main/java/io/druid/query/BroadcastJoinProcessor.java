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
import com.google.common.base.Throwables;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.BulkRow;

import java.io.IOException;
import java.util.Arrays;
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

  @JsonCreator
  public BroadcastJoinProcessor(
      @JacksonInject ObjectMapper mapper,
      @JacksonInject QueryConfig config,
      @JsonProperty("element") JoinElement element,
      @JsonProperty("hashLeft") boolean hashLeft,
      @JsonProperty("hashSignature") RowSignature hashSignature,
      @JsonProperty("prefixAlias") boolean prefixAlias,
      @JsonProperty("asArray") boolean asArray,
      @JsonProperty("outputAlias") List<String> outputAlias,
      @JsonProperty("outputColumns") List<String> outputColumns,
      @JsonProperty("maxOutputRow") int maxOutputRow,
      @JsonProperty("values") byte[] values
  )
  {
    super(config.getJoin(), prefixAlias, asArray, outputAlias, outputColumns, maxOutputRow);
    this.mapper = mapper;
    this.config = config;
    this.element = element;
    this.hashLeft = hashLeft;
    this.hashSignature = hashSignature;
    this.values = values;
  }

  @Override
  public BroadcastJoinProcessor withAsArray(boolean asArray)
  {
    return new BroadcastJoinProcessor(
        mapper,
        config,
        element,
        hashLeft,
        hashSignature,
        prefixAlias,
        asArray,
        outputAlias,
        outputColumns,
        maxOutputRow,
        values
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

  @Override
  public QueryRunner postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map response)
      {
        Preconditions.checkArgument(query instanceof Query.ArrayOutputSupport, "?? %s", query);
        Query.ArrayOutputSupport arrayQuery = (Query.ArrayOutputSupport) query;   // currently stream query only

        List<String> leftAlias = Arrays.asList(element.getLeftAlias());
        List<String> rightAlias = Arrays.asList(element.getRightAlias());
        List<String> leftJoinColumns = element.getLeftJoinColumns();
        List<String> rightJoinColumns = element.getRightJoinColumns();

        List<List<String>> names;
        LOG.info("Preparing broadcast join %s + %s", leftAlias, rightAlias);

        Sequence<BulkRow> rows = Sequences.simple(deserializeValue(mapper, values));
        Sequence<Object[]> queried = arrayQuery.array(QueryRunners.run(query, baseRunner));
        boolean stringAsRaw = arrayQuery.getContextBoolean(Query.STREAM_USE_RAW_UTF8, config.getSelect().isUseRawUTF8());
        Sequence<Object[]> hashing = Sequences.explode(rows, bulk -> Sequences.once(bulk.decompose(stringAsRaw)));

        List<String> hashColumns = hashSignature.getColumnNames();
        List<String> queryColumns = queried.columns();

        JoinAlias left;
        JoinAlias right;
        if (hashLeft) {
          left = toHashAlias(leftAlias, hashColumns, leftJoinColumns, hashing);
          right = toAlias(rightAlias, queryColumns, rightJoinColumns, query, queried);
          names = Arrays.asList(hashColumns, queryColumns);
        } else {
          left = toAlias(leftAlias, queryColumns, leftJoinColumns, query, queried);
          right = toHashAlias(rightAlias, hashColumns, rightJoinColumns, hashing);
          names = Arrays.asList(queryColumns, hashColumns);
        }
        JoinResult join = join(element.getJoinType(), left, right);

        List<String> outputAlias = getOutputAlias();
        if (outputAlias == null) {
          outputAlias = concatColumnNames(names, prefixAlias ? element.getAliases() : null);
        }
        return projection(join.iterator, outputAlias, query instanceof BaseAggregationQuery);
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
          Sequence<Object[]> queried
      )
      {
        return new JoinAlias(
            alias,
            columns,
            joinColumns,
            getCollations(query),
            GuavaUtils.indexOf(columns, joinColumns, true),
            Sequences.toIterator(queried),
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
    catch (IOException e) {
      throw Throwables.propagate(e);
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
  public RowSignature evolve(Query query, RowSignature schema)
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
        asArray,
        outputAlias,
        outputColumns,
        maxOutputRow,
        null
    );
  }

  @Override
  public String toString()
  {
    return "BroadcastJoinProcessor{" +
           "element=" + element +
           ", hashLeft=" + hashLeft +
           ", hashSignature=" + hashSignature +
           '}';
  }
}
