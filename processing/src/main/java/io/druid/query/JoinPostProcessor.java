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
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.collections.LimitedArrayLit;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.PrioritizedCallable;
import io.druid.data.input.MapBasedRow;
import io.druid.guice.annotations.Processing;
import io.druid.query.ordering.Comparators;
import io.druid.query.select.StreamRawQuery;
import io.druid.segment.ObjectArray;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class JoinPostProcessor extends PostProcessingOperator.UnionSupport implements PostProcessingOperator.Local
{
  private static final Logger log = new Logger(JoinPostProcessor.class);

  private final JoinQueryConfig config;
  private final JoinElement[] elements;
  private final boolean prefixAlias;
  private final QueryToolChestWarehouse warehouse;
  private final ExecutorService exec;

  private final SettableFuture<Hashed>[] hashed;

  @JsonCreator
  @SuppressWarnings("unchecked")
  public JoinPostProcessor(
      @JacksonInject JoinQueryConfig config,
      @JsonProperty("elements") List<JoinElement> elements,
      @JsonProperty("prefixAlias") boolean prefixAlias,
      @JacksonInject QueryToolChestWarehouse warehouse,
      @JacksonInject @Processing ExecutorService exec
  )
  {
    this.config = config;
    this.elements = elements.toArray(new JoinElement[elements.size()]);
    this.prefixAlias = prefixAlias;
    this.warehouse = warehouse;
    this.exec = exec;
    this.hashed = new SettableFuture[elements.size() + 1];
    for (int i = 0; i < hashed.length; i++) {
      hashed[i] = SettableFuture.create();
    }
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
      public Sequence run(final Query query, Map responseContext)
      {
        final int joinAliases = elements.length + 1;
        log.info("Running %d-way join processing %s", joinAliases, toAliases());
        final boolean[] hashing = new boolean[joinAliases];
        final boolean[] sorted = new boolean[joinAliases];
        final List<Sequence<Map<String, Object>>>[] sequencesList = new List[joinAliases];
        for (int i = 0; i < joinAliases; i++) {
          sequencesList[i] = Lists.<Sequence<Map<String, Object>>>newArrayList();
        }
        final MutableInt index = new MutableInt();
        Sequence<Pair<Query, Sequence>> sequences = baseQueryRunner.run(query, responseContext);
        sequences.accumulate(
            null, new Accumulator<Object, Pair<Query, Sequence>>()
            {
              @Override
              public Object accumulate(Object accumulated, Pair<Query, Sequence> in)
              {
                Query element = in.lhs;
                Sequence sequence = in.rhs;

                QueryToolChest toolChest = warehouse.getToolChest(element);
                if (toolChest != null) {
                  sequence = toolChest.toTabularFormat(element, sequence, null).getSequence();
                }
                sequencesList[index.intValue()].add(sequence);
                hashing[index.intValue()] = element.getContextBoolean("hash", false);
                if (query instanceof StreamRawQuery &&
                    toJoinColumns(index.intValue()).equals(((StreamRawQuery)query).getSortOn())) {
                  // uses ascending for join query (see JoinElement.toQuery)
                  sorted[index.intValue()] = true;
                }
                index.increment();
                return null;
              }
            }
        );

        Future[] joining = new Future[joinAliases];
        int i = 0;
        for (; i < index.intValue(); i++) {
          joining[i] = toList(i, hashing[i], sorted[i], sequencesList[i], toJoinColumns(i), exec);
        }
        for (; i < joining.length; i++) {
          joining[i] = hashed[i];
          hashing[i] = true;
        }
        try {
          return Sequences.simple(join(joining, hashing));
        }
        catch (ExecutionException e) {
          throw Throwables.propagate(e.getCause() == null ? e : e.getCause());
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  private Future toList(
      final int index,
      final boolean hashing,
      final boolean sorted,
      final List<Sequence<Map<String, Object>>> sequences,
      final List<String> joinColumns,
      final ExecutorService executor
  )
  {
    return executor.submit(
        new PrioritizedCallable.Background()
        {
          @Override
          public Object call()
          {
            Sequence<Map<String, Object>> sequence = Sequences.concat(sequences);
            List<Map<String, Object>> rows = Sequences.toList(
                sequence, new LimitedArrayLit<Map<String, Object>>(config.getMaxRows())
            );
            JoiningRow[] joining = toJoiningRow(rows, joinColumns, prefixAlias, index);
            if (hashing) {
              Hashed hashing = toHash(index, sort(index, joining));
              if (!hashed[index].set(hashing)) {
                throw new IllegalStateException("Failed to hash!");
              }
              return hashing;
            }
            if (!sorted) {
              joining = sort(index, joining);
            }
            return joining;
          }
        }
    );
  }

  private Hashed toHash(int index, JoiningRow[] sorted)
  {
    log.info("Hashing [%s] %d rows", toAlias(index), sorted.length);
    Map<ObjectArray<Comparable>, List<Map<String, Object>>> rights = Maps.newHashMap();
    Comparable[] prev = null;
    List<Map<String, Object>> rowList = Lists.newArrayList();
    for (JoiningRow row : sorted) {
      if (prev != null && !Arrays.equals(prev, row.joinKeys)) {
        rights.put(new ObjectArray<Comparable>(prev), rowList);
        rowList = Lists.newArrayList();
      }
      rowList.add(row.source);
      prev = row.joinKeys;
    }
    if (!rowList.isEmpty()) {
      rights.put(new ObjectArray<Comparable>(prev), rowList);
    }
    return new Hashed(rights);
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

  @SuppressWarnings("unchecked")
  private Iterable<Map<String, Object>> join(final Future[] rows, final boolean[] hashed)
      throws Exception
  {
    Preconditions.checkArgument(!hashed[0]);
    JoiningRow[] left = (JoiningRow[]) rows[0].get();
    Iterable<Map<String, Object>> iterable = ImmutableList.of();
    for (int i = 1; i < rows.length; i++) {
      if (hashed[i]) {
        iterable = hashJoin(left, (Hashed) rows[i].get(), i - 1);
      } else {
        iterable = join(left, (JoiningRow[]) rows[i].get(), i - 1);
      }
      if (i == rows.length - 1) {
        break;
      }
      left = sort(iterable, elements[i].getLeftJoinColumns(), i);
    }
    return iterable;
  }

  private Iterable<Map<String, Object>> join(final JoiningRow[] lefts, final JoiningRow[] rights, int index)
  {
    if (lefts.length == 0 && rights.length == 0) {
      return Collections.emptyList();
    }
    final JoinType type = elements[index].getJoinType();
    final int keyLength = elements[index].keyLength();
    log.info(
        "Starting %s join. %d rows [%s] + %d rows [%s]",
        type, lefts.length, toAlias(index), rights.length, toAlias(index + 1)
    );

    return new Iterable<Map<String, Object>>()
    {
      @Override
      public Iterator<Map<String, Object>> iterator()
      {
        final RowIterator left = new RowIterator(lefts, keyLength);
        final RowIterator right = new RowIterator(rights, keyLength);

        return new Iterator<Map<String, Object>>()
        {
          private Iterator<Map<String, Object>> iterator = Iterators.emptyIterator();

          @Override
          public boolean hasNext()
          {
            if (iterator.hasNext()) {
              return true;
            }
            switch (type) {
              case INNER:
                return inner();
              case LO:
                return leftOuter();
              case RO:
                return rightOuter();
            }
            throw new IllegalArgumentException();
          }

          private boolean inner()
          {
            while (!iterator.hasNext() && left.hasMore() && right.hasMore()) {
              final int compare = left.compareTo(right);
              if (compare == 0) {
                iterator = product(left.index, left.next(), right.index, right.next());
              } else if (compare > 0) {
                right.next();
              } else {
                left.next();
              }
            }
            return iterator.hasNext();
          }

          // a  a -> a a
          // a  b -> a x   : < 0
          // b  b -> b b
          private boolean leftOuter()
          {
            final int ls = left.index;
            while (!iterator.hasNext() && left.hasMore() && right.hasMore()) {
              final int compare = left.compareTo(right);
              if (compare == 0) {
                if (left.index > ls) {
                  iterator = left.iterateToCurrent(ls);
                } else {
                  iterator = product(left.index, left.next(), right.index, right.next());
                }
              } else if (compare > 0) {
                if (left.index > ls) {
                  iterator = left.iterateToCurrent(ls);
                } else {
                  right.next();
                }
              } else {
                left.next();
              }
            }
            if (!iterator.hasNext() && ls < left.limit) {
              iterator = left.iterateToEnd(ls);
            }
            return iterator.hasNext();
          }

          private boolean rightOuter()
          {
            final int rs = right.index;
            while (!iterator.hasNext() && left.hasMore() && right.hasMore()) {
              final int compare = left.compareTo(right);
              if (compare == 0) {
                if (right.index > rs) {
                  iterator = right.iterateToCurrent(rs);
                } else {
                  iterator = product(left.index, left.next(), right.index, right.next());
                }
              } else if (compare > 0) {
                right.next();
              } else {
                if (right.index > rs) {
                  iterator = right.iterateToCurrent(rs);
                } else {
                  left.next();
                }
              }
            }
            if (!iterator.hasNext() && rs < right.limit) {
              iterator = right.iterateToEnd(rs);
            }
            return iterator.hasNext();
          }

          @Override
          public Map<String, Object> next()
          {
            return iterator.next();
          }

          private Iterator<Map<String, Object>> product(final int ls, final int le, final int rs, final int re)
          {
            if (le - ls == 1 && re - rs == 1) {
              return Iterators.singletonIterator(lefts[ls].mergeRow(rights[rs]));
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
                if (ri >= re) {
                  li++;
                  ri = rs;
                }
                return lefts[li].mergeRow(rights[ri++]);
              }
            };
          }
        };
      }
    };
  }

  // from join result.. need prefix for key column
  private JoiningRow[] sort(Iterable<Map<String, Object>> result, List<String> columns, int index)
  {
    final List<Map<String, Object>> rows = Lists.newArrayList(result);
    log.info(".. sorting [%s] %d rows on %s", toAlias(index), rows.size(), columns);
    JoiningRow[] joining = toJoiningRow(rows, toKeyColumns(columns), false, index);
    long start = System.currentTimeMillis();
    Arrays.parallelSort(joining);
    log.info(".. sorted [%s] in %,d msec", toAlias(index), (System.currentTimeMillis() - start));
    return joining;
  }

  private JoiningRow[] sort(int index, JoiningRow[] rows)
  {
    long start = System.currentTimeMillis();
    Arrays.parallelSort(rows);
    log.info(".. sorted [%s] in %,d msec", toAlias(index), (System.currentTimeMillis() - start));
    return rows;
  }

  private JoiningRow[] toJoiningRow(
      List<Map<String, Object>> rows,
      List<String> columns,
      boolean prefixAlias,
      int index
  )
  {
    String alias = toAlias(index);

    long start = System.currentTimeMillis();
    final String prefix = alias + ".";
    final String[] array = columns.toArray(new String[columns.size()]);
    final JoiningRow[] sorted = new JoiningRow[rows.size()];
    for (int i = 0; i < sorted.length; i++) {
      Map<String, Object> row = rows.get(i);
      Comparable[] joinKey = new Comparable[array.length];
      for (int j = 0; j < array.length; j++) {
        joinKey[j] = (Comparable) row.get(array[j]);
      }
      if (prefixAlias) {
        Map<String, Object> prefixed = Maps.newHashMapWithExpectedSize(row.size());
        for (Map.Entry<String, Object> entry : row.entrySet()) {
          prefixed.put(prefix + entry.getKey(), entry.getValue());
        }
        row = prefixed;
      }
      sorted[i] = new JoiningRow(joinKey, row);
    }
    log.info(".. converted [%s] %d rows in %,d msec", alias, rows.size(), (System.currentTimeMillis() - start));
    return sorted;
  }

  private List<String> toKeyColumns(List<String> columns)
  {
    if (!prefixAlias) {
      return columns;
    }
    String prefix = toAlias(0) + ".";
    List<String> prefixed = Lists.newArrayList();
    for (String column : columns) {
      prefixed.add(prefix + column);
    }
    return prefixed;
  }

  @Override
  public boolean hasTabularOutput()
  {
    return true;
  }

  private static class JoiningRow implements Comparable<JoiningRow>
  {
    private final Comparable[] joinKeys;
    private final Map<String, Object> source;

    public JoiningRow(Comparable[] joinKeys, Map<String, Object> source)
    {
      this.joinKeys = joinKeys;
      this.source = source;
    }

    @Override
    public int compareTo(JoiningRow o)
    {
      for (int i = 0; i < joinKeys.length; i++) {
        int compare = Comparators.compareNF(joinKeys[i], o.joinKeys[i]);
        if (compare != 0) {
          return compare;
        }
      }
      return 0;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> mergeRow(JoiningRow row)
    {
      Map<String, Object> merged;
      if (MapBasedRow.supportInplaceUpdate(source)) {
        merged = source;
        merged.putAll(row.source);
      } else if (MapBasedRow.supportInplaceUpdate(row.source)) {
        merged = row.source;
        merged.putAll(source);
      } else {
        merged = Maps.newHashMap(source);
        merged.putAll(row.source);
      }
      return merged;
    }
  }

  private Iterable<Map<String, Object>> hashJoin(
      final JoiningRow[] lefts,
      final Hashed rights,
      final int index
  )
  {
    final JoinType type = elements[index].getJoinType();
    log.info(
        "Starting hash-%s join. %d rows [%s] + %d rows [%s]",
        type, lefts.length, toAlias(index), rights.size(), toAlias(index + 1)
    );
    Preconditions.checkArgument(type != JoinType.RO);
    if (lefts.length == 0 || rights.isEmpty()) {
      return Collections.emptyList();
    }
    final int keyLength = elements[index].keyLength();

    return new Iterable<Map<String, Object>>()
    {
      @Override
      public Iterator<Map<String, Object>> iterator()
      {
        final RowIterator left = new RowIterator(lefts, keyLength);

        return new Iterator<Map<String, Object>>()
        {
          private final ObjectArray<Comparable> wrapper = new ObjectArray<Comparable>(new Comparable[keyLength]);
          private Iterator<Map<String, Object>> iterator = Iterators.emptyIterator();

          @Override
          public boolean hasNext()
          {
            while (!iterator.hasNext() && left.hasMore()) {
              List<Map<String, Object>> match = rights.get(wrapper.pack(left.peek));
              if (match != null) {
                iterator = product(match);
              } else {
                if (type == JoinType.LO) {
                  iterator = left.iterate(left.index, left.next());
                } else {
                  left.next();
                }
              }
            }
            return iterator.hasNext();
          }

          private Iterator<Map<String, Object>> product(final List<Map<String, Object>> match)
          {
            final Iterator<Map<String, Object>> lefts = left.iterateToNext();
            if (!lefts.hasNext() || match.isEmpty()) {
              return Iterators.emptyIterator();
            }
            return new Iterator<Map<String, Object>>()
            {
              private Map<String, Object> left = lefts.next();
              private Iterator<Map<String, Object>> rights = match.iterator();

              @Override
              public boolean hasNext()
              {
                return lefts.hasNext() || rights.hasNext();
              }

              @Override
              public Map<String, Object> next()
              {
                if (rights.hasNext()) {
                  Map<String, Object> merged = Maps.newHashMap(left);
                  merged.putAll(rights.next());
                  return merged;
                }
                left = lefts.next();
                rights = match.iterator();
                return next();
              }
            };
          }

          @Override
          public Map<String, Object> next()
          {
            return iterator.next();
          }
        };
      }
    };
  }

  private static class RowIterator
  {
    private int index;

    private final JoiningRow[] rows;
    private final Comparable[] peek;

    private final int limit;
    private final int keyLength;

    private RowIterator(JoiningRow[] rows, int keyLength)
    {
      this.rows = rows;
      this.limit = rows.length;
      this.keyLength = keyLength;
      this.peek = new Comparable[keyLength];
      if (index < limit) {
        System.arraycopy(rows[index].joinKeys, 0, peek, 0, keyLength);
      }
    }

    private boolean hasMore()
    {
      return index < limit;
    }

    private int next()
    {
      boolean next = false;
      while (!next && ++index < limit) {
        for (int i = peek.length - 1; i >= 0; i--) {
          if (next || Comparators.compareNF(peek[i], readL(i)) != 0) {
            peek[i] = readL(i);
            next = true;
          }
        }
      }
      return index;
    }

    private Comparable readL(int i) {return rows[index].joinKeys[i];}

    private int compareTo(RowIterator other)
    {
      final Comparable[] otherKey = other.peek;
      for (int i = 0; i < keyLength; i++) {
        int compare = Comparators.compareNF(peek[i], otherKey[i]);
        if (compare != 0) {
          return compare;
        }
      }
      return 0;
    }

    private Iterator<Map<String, Object>> iterateToEnd(final int start)
    {
      return iterate(start, limit);
    }

    private Iterator<Map<String, Object>> iterateToCurrent(final int start)
    {
      return iterate(start, index);
    }

    private Iterator<Map<String, Object>> iterateToNext()
    {
      return iterate(index, next());
    }

    private Iterator<Map<String, Object>> iterate(final int start, final int end)
    {
      index = start;

      return new Iterator<Map<String, Object>>()
      {
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
  }

  static final Function<JoiningRow, Map<String, Object>> TO_ROW = new Function<JoiningRow, Map<String, Object>>()
  {
    @Override
    public Map<String, Object> apply(JoiningRow input)
    {
      return input.source;
    }
  };

  private static class Hashed
  {
    private final Map<ObjectArray<Comparable>, List<Map<String, Object>>> hashed;

    private Hashed(Map<ObjectArray<Comparable>, List<Map<String, Object>>> hashed) {this.hashed = hashed;}

    public boolean isEmpty()
    {
      return hashed.isEmpty();
    }

    public List<Map<String, Object>> get(ObjectArray<Comparable> key)
    {
      return hashed.get(key);
    }

    public int size()
    {
      return hashed.size();
    }
  }

  @SafeVarargs
  @VisibleForTesting
  final Iterable<Map<String, Object>> join(List<Map<String, Object>>... rows) throws Exception
  {
    @SuppressWarnings("unchecked")
    Future<JoiningRow[]>[] joining = new Future[rows.length];
    for (int i = 0; i < rows.length; i++) {
      List<String> joinColumns = i == 0 ? elements[0].getLeftJoinColumns() : elements[i - 1].getRightJoinColumns();
      joining[i] = Futures.immediateFuture(sort(rows[i], joinColumns, i));
    }
    return join(joining, new boolean[rows.length]);
  }

  @SafeVarargs
  @VisibleForTesting
  final Iterable<Map<String, Object>> hashJoin(List<Map<String, Object>>... rows) throws Exception
  {
    JoiningRow[] left = sort(rows[0], elements[0].getLeftJoinColumns(), 0);
    Iterable<Map<String, Object>> iterable = ImmutableList.of();
    for (int i = 1; i < rows.length; i++) {
      JoiningRow[] joiningRows = sort(rows[i], elements[i - 1].getRightJoinColumns(), i);
      iterable = hashJoin(left, toHash(i, joiningRows), i - 1);
      if (i == rows.length - 1) {
        break;
      }
      left = sort(Lists.newArrayList(iterable), elements[i].getLeftJoinColumns(), i);
    }
    return iterable;
  }
}
