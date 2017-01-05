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

package io.druid.query;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.guice.annotations.Processing;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class JoinPostProcessor implements PostProcessingOperator.UnionSupport
{
  private static final Logger log = new Logger(JoinPostProcessor.class);

  private final JoinType joinType;
  private final String leftAlias;
  private final String rightAlias;
  private final String[] leftJoinColumns;
  private final String[] rightJoinColumns;
  private final QueryToolChestWarehouse warehouse;
  private final ExecutorService exec;

  @JsonCreator
  public JoinPostProcessor(
      @JsonProperty("joinType") JoinType joinType,
      @JsonProperty("leftAlias") String leftAlias,
      @JsonProperty("leftJoinExpressions") List<String> leftJoinColumns,
      @JsonProperty("rightAlias") String rightAlias,
      @JsonProperty("rightJoinExpressions") List<String> rightJoinColumns,
      @JacksonInject QueryToolChestWarehouse warehouse,
      @JacksonInject @Processing ExecutorService exec
  )
  {
    this.joinType = joinType == null ? JoinType.INNER : joinType;
    this.leftAlias = Preconditions.checkNotNull(leftAlias);
    this.rightAlias = Preconditions.checkNotNull(rightAlias);
    this.leftJoinColumns = leftJoinColumns.toArray(new String[leftJoinColumns.size()]);
    this.rightJoinColumns = rightJoinColumns.toArray(new String[rightJoinColumns.size()]);
    this.warehouse = warehouse;
    this.exec = exec;
  }

  @Override
  public QueryRunner postProcess(QueryRunner baseQueryRunner)
  {
    throw new UnsupportedOperationException("should be used with union all query");
  }

  @Override
  public QueryRunner postProcess(final UnionAllQueryRunner baseQueryRunner)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        final List<Sequence<Map<String, Object>>> leftSequences = Lists.newArrayList();
        final List<Sequence<Map<String, Object>>> rightSequences = Lists.newArrayList();
        Sequence<Pair<Query, Sequence>> sequences = baseQueryRunner.run(query, responseContext);
        sequences.accumulate(
            null, new Accumulator<Object, Pair<Query, Sequence>>()
            {
              @Override
              public Object accumulate(
                  Object accumulated, Pair<Query, Sequence> in
              )
              {
                Query element = in.lhs;
                Sequence sequence = in.rhs;
                String dataSource = Iterables.getOnlyElement(element.getDataSource().getNames());
                if (leftAlias.equals(dataSource)) {
                  TabularFormat tabular = warehouse.getToolChest(element).toTabularFormat(sequence, null);
                  leftSequences.add(tabular.getSequence());
                } else if (rightAlias.equals(dataSource)) {
                  TabularFormat tabular = warehouse.getToolChest(element).toTabularFormat(sequence, null);
                  rightSequences.add(tabular.getSequence());
                } else {
                  throw new IllegalStateException("Invalid dataSource " + dataSource +
                                                  ".. should be one of " + Arrays.asList(leftAlias, rightAlias));
                }
                return null;
              }
            }
        );
        final Future<List<Map<String, Object>>> leftRowsFuture = toList(leftSequences);
        final Future<List<Map<String, Object>>> rightRowsFuture = toList(rightSequences);

        try {
          return Sequences.simple(join(leftRowsFuture.get(), rightRowsFuture.get(), joinType));
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  private Future<List<Map<String, Object>>> toList(final List<Sequence<Map<String, Object>>> sequences)
  {
    return exec.submit(
        new AbstractPrioritizedCallable<List<Map<String, Object>>>(0)
        {
          @Override
          public List<Map<String, Object>> call()
          {
            return Sequences.toList(Sequences.concat(sequences), Lists.<Map<String, Object>>newArrayList());
          }
        }
    );
  }

  @VisibleForTesting
  Iterable<Map<String, Object>> join(
      List<Map<String, Object>> leftRows,
      List<Map<String, Object>> rightRows,
      final JoinType type
  )
  {
    log.info("Starting joining left %d rows, right %d rows", leftRows.size(), rightRows.size());
    final JoiningRow[] lefts = sort(leftRows, leftJoinColumns);
    final JoiningRow[] rights = sort(rightRows, rightJoinColumns);

    final int sizeL = leftRows.size();
    final int sizeR = rightRows.size();
    return new Iterable<Map<String, Object>>()
    {
      @Override
      public Iterator<Map<String, Object>> iterator()
      {
        return new Iterator<Map<String, Object>>()
        {
          private Iterator<Map<String, Object>> iterator = Iterators.emptyIterator();

          private int indexL;
          private int indexR;
          private final String[] peekL = new String[leftJoinColumns.length];
          private final String[] peekR = new String[rightJoinColumns.length];

          {
            for (int i = 0; i < peekL.length; i++) {
              peekL[i] = readL(i);
            }
            for (int i = 0; i < peekR.length; i++) {
              peekR[i] = readR(i);
            }
          }

          @Override
          public boolean hasNext()
          {
            if (iterator.hasNext()) {
              return true;
            }
            switch (type) {
              case INNER: return inner();
              case LO: return leftOuter();
              case RO: return rightOuter();
            }
            throw new IllegalArgumentException();
          }

          private boolean inner()
          {
            while (!iterator.hasNext() && indexL < sizeL && indexR < sizeR) {
              final int compare = compare();
              if (compare == 0) {
                iterator = product(indexL, nextL(), indexR, nextR());
              } else if (compare > 0) {
                nextR();
              } else {
                nextL();
              }
            }
            return iterator.hasNext();
          }

          // a  a -> a a
          // a  b -> a x   : < 0
          // b  b -> b b
          private boolean leftOuter()
          {
            final int ls = indexL;
            while (!iterator.hasNext() && indexL < sizeL && indexR < sizeR) {
              final int compare = compare();
              if (compare == 0) {
                if (indexL > ls) {
                  iterator = leftOnly(ls, indexL);
                } else {
                  iterator = product(indexL, nextL(), indexR, nextR());
                }
              } else if (compare > 0) {
                if (indexL > ls) {
                  iterator = leftOnly(ls, indexL);
                } else {
                  nextR();
                }
              } else {
                nextL();
              }
            }
            if (!iterator.hasNext() && ls < sizeL) {
              iterator = leftOnly(ls, sizeL);
              indexL = sizeL;
            }
            return iterator.hasNext();
          }

          private boolean rightOuter()
          {
            final int rs = indexR;
            while (!iterator.hasNext() && indexL < sizeL && indexR < sizeR) {
              final int compare = compare();
              if (compare == 0) {
                if (indexR > rs) {
                  iterator = rightOnly(rs, indexR);
                } else {
                  iterator = product(indexL, nextL(), indexR, nextR());
                }
              } else if (compare > 0) {
                nextR();
              } else {
                if (indexR > rs) {
                  iterator = rightOnly(rs, indexR);
                } else {
                  nextL();
                }
              }
            }
            if (!iterator.hasNext() && rs < sizeR) {
              iterator = rightOnly(rs, sizeR);
              indexR = sizeR;
            }
            return iterator.hasNext();
          }

          @Override
          public Map<String, Object> next()
          {
            return iterator.next();
          }

          private String readL(int i) {return lefts[indexL].joinKeys[i];}

          private String readR(int i) {return rights[indexR].joinKeys[i];}

          private int nextL()
          {
            boolean next = false;
            while (!next && ++indexL < sizeL) {
              for (int i = peekL.length - 1; i >= 0; i--) {
                if (next || JoinPostProcessor.compare(peekL[i], readL(i)) != 0) {
                  peekL[i] = readL(i);
                  next = true;
                }
              }
            }
            return indexL;
          }

          private int nextR()
          {
            boolean next = false;
            while (!next && ++indexR < sizeR) {
              for (int i = peekR.length - 1; i >= 0; i--) {
                if (next || JoinPostProcessor.compare(peekR[i], readR(i)) != 0) {
                  peekR[i] = readR(i);
                  next = true;
                }
              }
            }
            return indexR;
          }

          private Iterator<Map<String, Object>> leftOnly(final int ls, final int le)
          {
            return toIterator(lefts, ls, le);
          }

          private Iterator<Map<String, Object>> rightOnly(final int rs, final int re)
          {
            return toIterator(rights, rs, re);
          }

          private Iterator<Map<String, Object>> product(final int ls, final int le, final int rs, final int re)
          {
            if (le - ls == 1 && re - rs == 1) {
              Map<String, Object> joined = Maps.newLinkedHashMap();
              joined.putAll(lefts[ls].source);
              joined.putAll(rights[rs].source);
              return Iterators.singletonIterator(joined);
            }
            return new Iterator<Map<String, Object>>()
            {
              int li = ls;
              int ri = rs;

              @Override
              public boolean hasNext()
              {
                return li < le - 1 || ri < re;
              }

              @Override
              public Map<String, Object> next()
              {
                Map<String, Object> joined = Maps.newLinkedHashMap();
                if (ri >= re) {
                  li++;
                  ri = rs;
                }
                joined.putAll(lefts[li].source);
                joined.putAll(rights[ri++].source);
                return joined;
              }
            };
          }

          private int compare()
          {
            for (int i = 0; i < peekL.length; i++) {
              int compare = JoinPostProcessor.compare(peekL[i], peekR[i]);
              if (compare != 0) {
                return compare;
              }
            }
            return 0;
          }
        };
      }
    };
  }

  private Iterator<Map<String, Object>> toIterator(final JoiningRow[] rows, final int start, final int end)
  {
    return new Iterator<Map<String, Object>>()
    {
      int index = start;

      @Override
      public boolean hasNext()
      {
        return index < end;
      }

      @Override
      public Map<String, Object> next()
      {
        return rows[index++].source;
      }
    };
  }

  private JoiningRow[] sort(List<Map<String, Object>> rows, String[] expressions)
  {
    String[] simple = new String[expressions.length];
    Expr[] complex = new Expr[expressions.length];
    boolean hasComplex = false;
    for (int i = 0; i < expressions.length; i++) {
      Expr expr = Parser.parse(expressions[i]);
      if (Evals.isIdentifier(expr)) {
        simple[i] = Evals.getIdentifier(expr);
      } else {
        complex[i] = expr;
        hasComplex = true;
      }
    }
    JoiningRow[] sorted = new JoiningRow[rows.size()];
    for (int i = 0; i < sorted.length; i++) {
      Map<String, Object> row = rows.get(i);
      String[] joinKey = new String[simple.length];
      for (int j = 0; j < simple.length; j++) {
        if (simple[j] != null) {
          joinKey[j] = (String) row.get(simple[j]);
        }
      }
      if (hasComplex) {
        Expr.NumericBinding binding = Parser.withMap(row);
        for (int j = 0; j < complex.length; j++) {
          if (complex[j] != null) {
            joinKey[j] = complex[j].eval(binding).asString();
          }
        }
      }
      sorted[i] = new JoiningRow(joinKey, row);
    }
    Arrays.sort(sorted);
    return sorted;
  }

  private static class JoiningRow implements Comparable<JoiningRow>
  {
    final Map<String, Object> source;
    final String[] joinKeys;

    private JoiningRow(String[] joinKeys, Map<String, Object> source)
    {
      this.joinKeys = joinKeys;
      this.source = source;
    }

    @Override
    public int compareTo(JoiningRow o)
    {
      for (int i = 0; i < joinKeys.length; i++) {
        int compare = JoinPostProcessor.compare(joinKeys[i], o.joinKeys[i]);
        if (compare != 0) {
          return compare;
        }
      }
      return 0;
    }
  }

  static final int LEFT_IS_GREATER = 1;
  static final int RIGHT_IS_GREATER = -1;

  private static int compare(String d1, String d2)
  {
    if (d1 == d2) {
      return 0;
    }
    if (d1 == null) {
      return RIGHT_IS_GREATER;
    }
    if (d2 == null) {
      return LEFT_IS_GREATER;
    }
    return d1.compareTo(d2);
  }
}
