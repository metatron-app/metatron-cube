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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.druid.common.IntTagged;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.concurrent.PrioritizedCallable;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.JoinQuery.CommonJoinHolder;
import io.druid.query.PostProcessingOperator.Local;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class JoinPostProcessor extends CommonJoinProcessor implements PostProcessingOperator.UnionSupport, Local
{
  private final JoinElement[] elements;

  @JsonCreator
  @SuppressWarnings("unchecked")
  public JoinPostProcessor(
      @JacksonInject JoinQueryConfig config,
      @JsonProperty("elements") List<JoinElement> elements,
      @JsonProperty("prefixAlias") boolean prefixAlias,
      @JsonProperty("asArray") boolean asArray,
      @JsonProperty("outputColumns") List<String> outputColumns,
      @JsonProperty("maxOutputRow") int maxOutputRow
  )
  {
    super(config, prefixAlias, asArray, outputColumns, maxOutputRow);
    this.elements = elements.toArray(new JoinElement[0]);
  }

  @Override
  public JoinPostProcessor withAsArray(boolean asArray)
  {
    return new JoinPostProcessor(config, Arrays.asList(elements), prefixAlias, asArray, outputColumns, maxOutputRow);
  }

  @Override
  public QueryRunner postProcess(QueryRunner baseQueryRunner)
  {
    throw new UnsupportedOperationException("should be used with union all query");
  }

  @Override
  public QueryRunner postProcess(final UnionAllQueryRunner baseRunner, final ExecutorService exec)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        final int joinAliases = elements.length + 1;
        LOG.info("Running %d-way join processing %s", joinAliases, toAliases());
        final List<String> aliases = Lists.newArrayList();
        for (int i = 0; i < joinAliases; i++) {
          aliases.add(toAlias(i));
        }

        final List<Pair<Query, Sequence>> pairs = Sequences.toList(baseRunner.run(query, responseContext));
        final List<IntTagged<Callable<JoinAlias>>> nested = Lists.newArrayList();
        final List<IntTagged<Callable<JoinAlias>>> nonNested = Lists.newArrayList();
        for (int i = 0; i < pairs.size(); i++) {
          Pair<Query, Sequence> in = pairs.get(i);
          Query.ArrayOutputSupport alias = (Query.ArrayOutputSupport) in.lhs;
          Callable<JoinAlias> callable = toJoinAlias(toAlias(i), alias, toJoinColumns(i), alias.array(in.rhs));
          if (Queries.isNestedQuery(in.lhs)) {
            nested.add(IntTagged.of(i, callable));
          } else {
            nonNested.add(IntTagged.of(i, callable));
          }
        }
        // nested first
        final Future[] joining = new Future[joinAliases];
        for (IntTagged<Callable<JoinAlias>> callable : nested) {
          joining[callable.tag] = Execs.excuteDirect(callable.value);
        }
        for (IntTagged<Callable<JoinAlias>> callable : nonNested) {
          joining[callable.tag] = exec.submit(callable.value);
        }
        final CommonJoinHolder joinQuery = (CommonJoinHolder) query;
        final int estimatedNumRows = query.getContextInt(JoinQuery.CARDINALITY, -1);
        try {
          JoinResult join = join(joining, estimatedNumRows);
          joinQuery.setCollation(join.collation);

          List<List<String>> names = GuavaUtils.transform(pairs, pair -> pair.rhs.columns());
          return projection(join.iterator, concatColumnNames(names, prefixAlias ? aliases : null));
        }
        catch (Throwable t) {
          if (t instanceof ExecutionException && t.getCause() != null) {
            t = t.getCause();
          }
          joinQuery.setException(t);
          throw Throwables.propagate(t);
        }
      }
    };
  }

  private PrioritizedCallable<JoinAlias> toJoinAlias(
      final String alias,
      final Query.ArrayOutputSupport<?> source,
      final List<String> joinColumns,
      final Sequence<Object[]> sequence
  )
  {
    final List<String> aliases = Arrays.asList(alias);
    final List<String> columnNames = sequence.columns();
    final int[] indices = GuavaUtils.indexOf(columnNames, joinColumns, true);
    if (indices == null) {
      throw new IAE("Cannot find join column %s in %s", joinColumns, columnNames);
    }
    final int cardinality = source.getContextInt(JoinQuery.CARDINALITY, -1);
    final boolean hashing = source.getContextBoolean(JoinQuery.HASHING, false);
    if (hashing) {
      return new PrioritizedCallable.Background<JoinAlias>()
      {
        @Override
        public JoinAlias call()
        {
          return new JoinAlias(aliases, columnNames, joinColumns, indices, Sequences.toIterator(sequence));
        }
      };
    }
    return new PrioritizedCallable.Background<JoinAlias>()
    {
      @Override
      public JoinAlias call()
      {
        final Iterator<Object[]> rows = Sequences.toIterator(sequence);
        return new JoinAlias(aliases, columnNames, joinColumns, getCollation(source), indices, rows, cardinality);
      }
    };
  }

  private JoinType toJoinType(int index)
  {
    return elements[index == 0 ? 0 : index - 1].getJoinType();
  }

  private String toAlias(int index)
  {
    return index == 0 ? elements[0].getLeftAlias() : elements[index - 1].getRightAlias();
  }

  private List<String> toJoinColumns(int index)
  {
    return index == 0 ? elements[0].getLeftJoinColumns() : elements[index - 1].getRightJoinColumns();
  }

  private List<String> toAliases()
  {
    List<String> aliases = Lists.newArrayList();
    aliases.add(elements[0].getLeftAlias());
    for (JoinElement element : elements) {
      aliases.add(element.getRightAlias());
    }
    return aliases;
  }

  @VisibleForTesting
  final JoinResult join(final Future<JoinAlias>[] futures, int estimatedNumRows) throws Exception
  {
    JoinAlias left = futures[0].get();
    JoinAlias right = futures[1].get();
    JoinResult result = join(left, right, 0);
    List<String> alias = GuavaUtils.concat(left.alias, right.alias);
    List<String> columns = GuavaUtils.concat(left.columns, right.columns);
    List<String> joinColumns = elements[0].getLeftJoinColumns();
    for (int i = 2; i < futures.length; i++) {
      left = new JoinAlias(
          alias, columns, joinColumns, Suppliers.ofInstance(result.collation),
          GuavaUtils.indexOf(columns, joinColumns), result.iterator, estimatedNumRows
      );
      right = futures[i].get();
      result = join(left, right, i - 1);
      alias = GuavaUtils.concat(alias, right.alias);
      columns = GuavaUtils.concat(columns, right.columns);
      joinColumns = elements[i - 1].getLeftJoinColumns();
    }
    return result;
  }

  @VisibleForTesting
  final JoinResult join(JoinAlias left, JoinAlias right, int index)
  {
    return join(elements[index].getJoinType(), left, right);
  }
}
