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
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.guice.annotations.Processing;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class JoinPostProcessor implements PostProcessingOperator.UnionSupport
{
  private static final Logger log = new Logger(JoinPostProcessor.class);

  private final JoinType joinType;
  private final Set<String> leftAliases;
  private final Set<String> rightAliases;
  private final String[] leftJoinColumns;
  private final String[] rightJoinColumns;
  private final QueryToolChestWarehouse warehouse;
  private final ExecutorService exec;

  @JsonCreator
  public JoinPostProcessor(
      @JsonProperty("joinType") JoinType joinType,
      @JsonProperty("leftAliases") List<String> leftAliases,
      @JsonProperty("leftJoinColumns") List<String> leftJoinColumns,
      @JsonProperty("rightAliases") List<String> rightAliases,
      @JsonProperty("rightJoinColumns") List<String> rightJoinColumns,
      @JacksonInject QueryToolChestWarehouse warehouse,
      @JacksonInject @Processing ExecutorService exec
  )
  {
    this.joinType = joinType == null ? JoinType.INNER : joinType;
    this.leftAliases = Sets.newHashSet(leftAliases);
    this.rightAliases = Sets.newHashSet(rightAliases);
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
                if (leftAliases.contains(dataSource)) {
                  TabularFormat tabular = warehouse.getToolChest(element).toTabularFormat(sequence, null);
                  leftSequences.add(tabular.getSequence());
                } else if (rightAliases.contains(dataSource)) {
                  TabularFormat tabular = warehouse.getToolChest(element).toTabularFormat(sequence, null);
                  rightSequences.add(tabular.getSequence());
                } else {
                  throw new IllegalStateException("Invalid dataSource " + dataSource);
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
      final List<Map<String, Object>> leftRows,
      final List<Map<String, Object>> rightRows,
      final JoinType type
  )
  {
    log.info("Starting joining left %d rows, right %d rows", leftRows.size(), rightRows.size());
    Collections.sort(leftRows, dimensionOrdering(leftJoinColumns));
    Collections.sort(rightRows, dimensionOrdering(rightJoinColumns));

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

          private String readL(int i) {return (String) leftRows.get(indexL).get(leftJoinColumns[i]);}

          private String readR(int i) {return (String) rightRows.get(indexR).get(rightJoinColumns[i]);}

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
            return leftRows.subList(ls, le).iterator();
          }

          private Iterator<Map<String, Object>> rightOnly(final int rs, final int re)
          {
            return rightRows.subList(rs, re).iterator();
          }

          private Iterator<Map<String, Object>> product(final int ls, final int le, final int rs, final int re)
          {
            if (le - ls == 1 && re - rs == 1) {
              Map<String, Object> joined = Maps.newLinkedHashMap();
              joined.putAll(leftRows.get(ls));
              joined.putAll(rightRows.get(rs));
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
                joined.putAll(leftRows.get(li));
                joined.putAll(rightRows.get(ri++));
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

  static final int LEFT_IS_GREATER = 1;
  static final int RIGHT_IS_GREATER = -1;

  private static Comparator<Map<String, Object>> dimensionOrdering(final String[] dimensions)
  {
    return new Comparator<Map<String, Object>>()
    {
      @Override
      public int compare(Map<String, Object> o1, Map<String, Object> o2)
      {
        for (String dimension : dimensions) {
          final String d1 = (String) o1.get(dimension);
          final String d2 = (String) o2.get(dimension);
          int compare = JoinPostProcessor.compare(d1, d2);
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      }
    };
  }

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
