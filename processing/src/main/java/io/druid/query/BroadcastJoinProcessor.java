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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.BulkRow;
import io.druid.java.util.common.guava.Sequence;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class BroadcastJoinProcessor extends CommonJoinProcessor implements PostProcessingOperator
{
  private final JoinElement element;
  private final boolean hashLeft;
  private final RowSignature hashSignature;
  private final Sequence<BulkRow> values;

  @JsonCreator
  public BroadcastJoinProcessor(
      @JacksonInject JoinQueryConfig config,
      @JsonProperty("element") JoinElement element,
      @JsonProperty("leftHashed") boolean hashLeft,
      @JsonProperty("hashSignature") RowSignature hashSignature,
      @JsonProperty("prefixAlias") boolean prefixAlias,
      @JsonProperty("asArray") boolean asArray,
      @JsonProperty("outputColumns") List<String> outputColumns,
      @JsonProperty("values") Sequence<BulkRow> values

  )
  {
    super(config, prefixAlias, asArray, outputColumns, -1);
    this.element = element;
    this.hashLeft = hashLeft;
    this.hashSignature = hashSignature;
    this.values = values;
  }

  @Override
  public BroadcastJoinProcessor withAsArray(boolean asArray)
  {
    return new BroadcastJoinProcessor(
        config,
        element,
        hashLeft,
        hashSignature,
        prefixAlias,
        asArray,
        outputColumns,
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
  public Sequence<BulkRow> getValues()
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
        JoinQuery.BroadcastJoinHolder joinQuery = (JoinQuery.BroadcastJoinHolder) query;
        Preconditions.checkArgument(joinQuery.getQuery() instanceof Query.ArrayOutputSupport, "?? %s", query);

        Query.ArrayOutputSupport arrayQuery = (Query.ArrayOutputSupport) joinQuery.getQuery();
        List<String> queryColumns = arrayQuery.estimatedOutputColumns();
        List<String> hashColumns = hashSignature.getColumnNames();

        Sequence<Object[]> queried = arrayQuery.array(QueryRunners.run(joinQuery, baseRunner));
        Sequence<Object[]> hashing = Sequences.explode(values, bulk -> Sequences.once(bulk.decompose()));

        List<String> leftAlias = Arrays.asList(element.getLeftAlias());
        List<String> rightAlias = Arrays.asList(element.getRightAlias());
        List<String> leftJoinColumns = element.getLeftJoinColumns();
        List<String> rightJoinColumns = element.getRightJoinColumns();

        List<List<String>> columnsNames;
        JoinAlias left;
        JoinAlias right;
        if (hashLeft) {
          left = toHashAlias(leftAlias, hashColumns, leftJoinColumns, hashing);
          right = toAlias(rightAlias, queryColumns, rightJoinColumns, query, queried);
          columnsNames = Arrays.asList(hashColumns, queryColumns);
        } else {
          left = toAlias(leftAlias, queryColumns, leftJoinColumns, query, queried);
          right = toHashAlias(rightAlias, hashColumns, rightJoinColumns, hashing);
          columnsNames = Arrays.asList(queryColumns, hashColumns);
        }
        JoinResult join = join(element.getJoinType(), left, right);

        return projection(join.iterator, concatColumnNames(columnsNames, prefixAlias ? element.getAliases() : null));
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
            getCollation(query),
            GuavaUtils.indexOf(columns, joinColumns, true),
            Sequences.toIterator(queried),
            -1
        );
      }
    };
  }

  public List<String> estimatedOutputColumns(List<String> queryColumns)
  {
    if (outputColumns != null) {
      return outputColumns;
    }
    if (hashLeft) {
      return GuavaUtils.concat(hashSignature.getColumnNames(), queryColumns);
    } else {
      return GuavaUtils.concat(queryColumns, hashSignature.getColumnNames());
    }
  }

  public RowSignature estimatedSchema(Query query, QuerySegmentWalker segmentWalker)
  {
    RowSignature signature;
    if (hashLeft) {
      signature = hashSignature.concat(Queries.relaySchema(query, segmentWalker));
    } else {
      signature = Queries.relaySchema(query, segmentWalker).concat(hashSignature);
    }
    if (outputColumns != null) {
      signature = signature.retain(outputColumns);
    }
    return signature;
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
