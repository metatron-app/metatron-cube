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
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.data.input.MapBasedRow;
import io.druid.guice.annotations.Processing;
import org.python.google.common.util.concurrent.Futures;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class JoinPostProcessor extends PostProcessingOperator.UnionSupport
{
  private static final Logger log = new Logger(JoinPostProcessor.class);

  private final JoinElement[] elements;
  private final QueryToolChestWarehouse warehouse;
  private final ExecutorService exec;

  @JsonCreator
  public JoinPostProcessor(
      @JsonProperty("elements") List<JoinElement> elements,
      @JacksonInject QueryToolChestWarehouse warehouse,
      @JacksonInject @Processing ExecutorService exec
  )
  {
    this.elements = elements.toArray(new JoinElement[elements.size()]);
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
        final int joinAliases = elements.length + 1;
        final List<Sequence<Map<String, Object>>>[] sequencesList = new List[joinAliases];
        for (int i = 0; i < joinAliases; i++) {
          sequencesList[i] = Lists.<Sequence<Map<String, Object>>>newArrayList();
        }
        Sequence<Pair<Query, Sequence>> sequences = baseQueryRunner.run(query, responseContext);
        sequences.accumulate(
            null, new Accumulator<Object, Pair<Query, Sequence>>()
            {
              int index;
              @Override
              public Object accumulate(
                  Object accumulated, Pair<Query, Sequence> in
              )
              {
                Query element = in.lhs;
                Sequence sequence = in.rhs;

                TabularFormat tabular = warehouse.getToolChest(element).toTabularFormat(sequence, null);
                sequencesList[index++].add(tabular.getSequence());
                return null;
              }
            }
        );

        @SuppressWarnings("unchecked")
        Future<JoiningRow[]>[] joining = new Future[joinAliases];
        for (int i = 0; i < sequencesList.length; i++) {
          if (i == 0) {
            joining[i] = toList(sequencesList[i], elements[0].getLeftJoinColumns(), exec);
          } else {
            joining[i] = toList(sequencesList[i], elements[i - 1].getRightJoinColumns(), exec);
          }
        }
        try {
          return Sequences.simple(join(joining));
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  private Future<JoiningRow[]> toList(
      final List<Sequence<Map<String, Object>>> sequences,
      final List<String> joinColumns,
      final ExecutorService executor
  )
  {
    return executor.submit(
        new AbstractPrioritizedCallable<JoiningRow[]>(0)
        {
          @Override
          public JoiningRow[] call()
          {
            Sequence<Map<String, Object>> sequence = Sequences.concat(sequences);
            List<Map<String, Object>> rows = Sequences.toList(sequence, Lists.<Map<String, Object>>newArrayList());
            return sort(rows, joinColumns);
          }
        }
    );
  }

  @SafeVarargs
  @VisibleForTesting
  final Iterable<Map<String, Object>> join(List<Map<String, Object>>... rows) throws Exception
  {
    @SuppressWarnings("unchecked")
    Future<JoiningRow[]>[] joining = new Future[rows.length];
    for (int i = 0; i < rows.length; i++) {
      List<String> joinColumns = i == 0  ? elements[0].getLeftJoinColumns() : elements[i - 1].getRightJoinColumns();
      joining[i] = Futures.immediateFuture(sort(rows[i], joinColumns));
    }
    return join(joining);
  }

  private Iterable<Map<String, Object>> join(final Future<JoiningRow[]>[] rows)
      throws Exception
  {
    JoiningRow[] left = rows[0].get();
    Iterable<JoiningRow> iterable = ImmutableList.of();
    for (int i = 1; i < rows.length; i++) {
      iterable = join(left, rows[i].get(), elements[i - 1]);
      if (i == rows.length - 1) {
        break;
      }
      left = sort(Iterables.toArray(iterable, JoiningRow.class), elements[i].getLeftJoinColumns());
    }
    return Iterables.transform(iterable, TO_ROW);
  }

  private Iterable<JoiningRow> join(final JoiningRow[] lefts, final JoiningRow[] rights, JoinElement element)
  {
    if (lefts.length == 0 && rights.length == 0) {
      return Collections.emptyList();
    }
    final JoinType type = element.getJoinType();
    final int joinColumnLen = element.getLeftJoinColumns().size();
    log.info("Starting %s joining left %d rows + right %d rows", type, lefts.length, rights.length);

    final int sizeL = lefts.length;
    final int sizeR = rights.length;
    return new Iterable<JoiningRow>()
    {
      @Override
      public Iterator<JoiningRow> iterator()
      {
        return new Iterator<JoiningRow>()
        {
          private Iterator<JoiningRow> iterator = Iterators.emptyIterator();

          private int indexL;
          private int indexR;
          private final String[] peekL = new String[joinColumnLen];
          private final String[] peekR = new String[joinColumnLen];

          {
            if (sizeL > 0) {
              for (int i = 0; i < peekL.length; i++) {
                peekL[i] = readL(i);
              }
            }
            if (sizeR > 0) {
              for (int i = 0; i < peekR.length; i++) {
                peekR[i] = readR(i);
              }
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
          public JoiningRow next()
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

          private Iterator<JoiningRow> leftOnly(final int ls, final int le)
          {
            return toIterator(lefts, ls, le);
          }

          private Iterator<JoiningRow> rightOnly(final int rs, final int re)
          {
            return toIterator(rights, rs, re);
          }

          private Iterator<JoiningRow> product(final int ls, final int le, final int rs, final int re)
          {
            if (le - ls == 1 && re - rs == 1) {
              return Iterators.singletonIterator(lefts[ls].merge(rights[rs]));
            }
            return new Iterator<JoiningRow>()
            {
              int li = ls;
              int ri = rs;

              @Override
              public boolean hasNext()
              {
                return li < le - 1 || ri < re;
              }

              @Override
              public JoiningRow next()
              {
                if (ri >= re) {
                  li++;
                  ri = rs;
                }
                return lefts[li].merge(rights[ri++]);
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

  private Iterator<JoiningRow> toIterator(final JoiningRow[] rows, final int start, final int end)
  {
    return new Iterator<JoiningRow>()
    {
      int index = start;

      @Override
      public boolean hasNext()
      {
        return index < end;
      }

      @Override
      public JoiningRow next()
      {
        return rows[index++];
      }
    };
  }

  private JoiningRow[] sort(List<Map<String, Object>> rows, List<String> columns)
  {
    final String[] array = columns.toArray(new String[columns.size()]);
    final JoiningRow[] sorted = new JoiningRow[rows.size()];
    for (int i = 0; i < sorted.length; i++) {
      Map<String, Object> row = rows.get(i);
      String[] joinKey = new String[array.length];
      for (int j = 0; j < array.length; j++) {
        joinKey[j] = (String) row.get(array[j]);
      }
      sorted[i] = new JoiningRow(joinKey, row);
    }
    Arrays.sort(sorted);
    return sorted;
  }

  private JoiningRow[] sort(JoiningRow[] rows, List<String> columns)
  {
    log.info("Sorting intermediate results on %s", columns);
    final String[] array = columns.toArray(new String[columns.size()]);
    for (int i = 0; i < rows.length; i++) {
      Map<String, Object> row = rows[i].source;
      String[] joinKey = new String[array.length];
      for (int j = 0; j < array.length; j++) {
        joinKey[j] = (String) row.get(array[j]);
      }
      rows[i] = new JoiningRow(joinKey, row);
    }
    Arrays.sort(rows);
    return rows;
  }

  @Override
  public boolean hasTabularOutput()
  {
    return true;
  }

  private static class JoiningRow implements Comparable<JoiningRow>
  {
    private final String[] joinKeys;
    private final Map<String, Object> source;

    public JoiningRow(String[] joinKeys, Map<String, Object> source)
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

    @SuppressWarnings("unchecked")
    public JoiningRow merge(JoiningRow row)
    {
      Map<String, Object> merged;
      if (MapBasedRow.supportInplaceUpdate(source)) {
        source.putAll(row.source);
        merged = source;
      } else if (MapBasedRow.supportInplaceUpdate(row.source)) {
        row.source.putAll(source);
        merged = row.source;
      } else {
        merged = Maps.newHashMap(source);
        merged.putAll(row.source);
      }
      return new JoiningRow(joinKeys, merged);
    }
  }

  static final Function<JoiningRow, Map<String, Object>> TO_ROW = new Function<JoiningRow, Map<String, Object>>()
  {
    @Override
    public Map<String, Object> apply(JoiningRow input)
    {
      return input.source;
    }
  };

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
