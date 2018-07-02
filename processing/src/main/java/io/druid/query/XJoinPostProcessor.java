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
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.Futures;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.guice.annotations.Processing;
import io.druid.query.ordering.Comparators;
import org.apache.commons.lang.mutable.MutableInt;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class XJoinPostProcessor extends PostProcessingOperator.UnionSupport
{
  private static final Logger log = new Logger(XJoinPostProcessor.class);

  private final JoinElement[] elements;
  private final boolean prefixAlias;
  private final boolean asArray;
  private final ExecutorService exec;

  @JsonCreator
  @SuppressWarnings("unchecked")
  public XJoinPostProcessor(
      @JsonProperty("elements") List<JoinElement> elements,
      @JsonProperty("prefixAlias") boolean prefixAlias,
      @JsonProperty("asArray") boolean asArray,
      @JacksonInject @Processing ExecutorService exec
  )
  {
    this.elements = elements.toArray(new JoinElement[elements.size()]);
    this.asArray = asArray;
    this.prefixAlias = prefixAlias;
    this.exec = exec;
  }

  public boolean asArray()
  {
    return asArray;
  }

  public XJoinPostProcessor withAsArray(boolean asArray)
  {
    return new XJoinPostProcessor(Arrays.asList(elements), prefixAlias, asArray, exec);
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
        log.info("Running %d-way join processing %s", joinAliases, toAliases());
        final List<String> aliases = Lists.newArrayList();
        final List<List<String>> aliasColumnsNames = Lists.newArrayList();
        final List<Sequence<Object[]>>[] sequencesList = new List[joinAliases];
        for (int i = 0; i < joinAliases; i++) {
          aliases.add(toAlias(i));
          sequencesList[i] = Lists.<Sequence<Object[]>>newArrayList();
        }
        final boolean[] sorted = new boolean[joinAliases];
        final MutableInt indexer = new MutableInt();
        Sequence<Pair<Query, Sequence>> sequences = baseQueryRunner.run(query, responseContext);
        sequences.accumulate(
            null, new Accumulator<Object, Pair<Query, Sequence>>()
            {
              @Override
              public Object accumulate(
                  Object accumulated, Pair<Query, Sequence> in
              )
              {
                sorted[indexer.intValue()] = in.lhs.getContextBoolean(JoinElement.SORTED_CONTEXT_KEY, false);
                Query.ArrayOutputSupport query = (Query.ArrayOutputSupport)in.lhs;
                aliasColumnsNames.add(query.estimatedOutputColumns());
                sequencesList[indexer.intValue()].add(query.array(in.rhs));
                indexer.increment();
                return null;
              }
            }
        );

        final Future[] joining = new Future[joinAliases];
        for (int i = 0; i < indexer.intValue(); i++) {
          joining[i] = toJoinAlias(
              !sorted[i],
              toAlias(i),
              sequencesList[i],
              aliasColumnsNames.get(i),
              toJoinColumns(i),
              exec
          );
        }
        try {
          Iterator join = join(joining);
          if (!asArray) {
            Iterator converted = Iterators.transform(join, converter(aliasColumnsNames, prefixAlias ? aliases : null));
            if (join instanceof Closeable) {
              converted = GuavaUtils.withResource(converted, (Closeable) join);
            }
            join = converted;
          }
          return Sequences.once(join);
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

  private Future toJoinAlias(
      final boolean sort,
      final String alias,
      final List<Sequence<Object[]>> sequences,
      final List<String> columnNames,
      final List<String> joinColumns,
      final ExecutorService executor
  )
  {
    final int[] indices = GuavaUtils.indexOf(columnNames, joinColumns);
    final Sequence<Object[]> sequence = Sequences.concat(sequences);
    if (!sort) {
      return Futures.immediateFuture(
          new JoinAlias(Arrays.asList(alias), columnNames, indices, Sequences.toIterator(sequence))
      );
    }
    return executor.submit(
        new AbstractPrioritizedCallable(0)
        {
          @Override
          public Object call()
          {
            return new JoinAlias(
                Arrays.asList(alias), columnNames, indices, sort(sequence, indices).iterator()
            );
          }
        }
    );
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
  private Iterator<Object[]> join(final Future[] futures)
      throws Exception
  {
    JoinAlias left = (JoinAlias) futures[0].get();
    Iterator<Object[]> iterator = Iterators.emptyIterator();
    for (int i = 1; i < futures.length; i++) {
      JoinAlias right = (JoinAlias) futures[i].get();
      log.info("... start joining %s to %s", left.alias, right.alias);
      iterator = join(left, right, i - 1);
      if (i == futures.length - 1) {
        break;
      }
      List<String> alias = GuavaUtils.concat(left.alias, right.alias);
      List<String> columns = GuavaUtils.concat(left.columns, right.columns);
      int[] indices = GuavaUtils.indexOf(columns, elements[i].getLeftJoinColumns());
      // todo sketch on next join column & partitioned sort
      left = new JoinAlias(alias, columns, indices, sort(Sequences.once(iterator), indices).iterator());
    }
    return iterator;
  }

  private Iterator<Object[]> join(final JoinAlias leftAlias, final JoinAlias rightAlias, final int index)
  {
    return new GuavaUtils.CloseableIterator<Object[]>()
    {
      @Override
      public void close() throws IOException
      {
        if (leftAlias.rows instanceof Closeable) {
          ((Closeable) leftAlias.rows).close();
        }
        if (rightAlias.rows instanceof Closeable) {
          ((Closeable) rightAlias.rows).close();
        }
      }

      private Iterator<Object[]> iterator = Iterators.emptyIterator();

      @Override
      public boolean hasNext()
      {
        while (iterator != null && !iterator.hasNext()) {
          iterator = joinPartition(leftAlias, rightAlias, index);
        }
        return iterator != null;
      }

      @Override
      public Object[] next()
      {
        return iterator.next();
      }
    };
  }

  private Partition left;
  private Partition right;

  private Iterator<Object[]> joinPartition(final JoinAlias leftAlias, final JoinAlias rightAlias, final int index)
  {
    final JoinType type = elements[index].getJoinType();

    if (left == null) {
      left = leftAlias.next();
    }
    if (right == null) {
      right = rightAlias.next();
    }
    while (left != null && right != null) {
      final int compare = Comparators.compareNF(left.joinKey, right.joinKey);
      if (compare == 0) {
        Iterator<Object[]> product = product(left.rows, right.rows);
        left = leftAlias.next();
        right = rightAlias.next();
        return product;
      }
      switch (type) {
        case INNER:
          if (compare < 0) {
            left = leftAlias.next();
          } else {
            right = rightAlias.next();
          }
          continue;
        case LO:
          if (compare < 0) {
            Iterator<Object[]> lo = lo(leftAlias.outer(right.joinKey).rows, rightAlias.indices.length);
            left = leftAlias.next();
            return lo;
          } else {
            rightAlias.skip(left.joinKey);
            right = rightAlias.next();
          }
          continue;
        case RO:
          if (compare < 0) {
            leftAlias.skip(right.joinKey);
            left = leftAlias.next();
          } else {
            Iterator<Object[]> ro = ro(leftAlias.indices.length, rightAlias.outer(left.joinKey).rows);
            right = rightAlias.next();
            return ro;
          }
          continue;
        default:
          throw new UnsupportedOperationException("not supported type " + type);
      }
    }
    if (type == JoinType.LO) {
      for (; left != null; left = leftAlias.next()) {
        lo(left.rows, rightAlias.indices.length);
      }
    } else if (type == JoinType.RO) {
      for (; right != null; right = rightAlias.next()) {
        ro(leftAlias.indices.length, right.rows);
      }
    }
    left = null;
    right = null;
    return null;
  }

  private static class Partition
  {
    final Comparable[] joinKey;
    final List<Object[]> rows;

    private Partition(Comparable[] joinKey, List<Object[]> rows)
    {
      this.joinKey = joinKey;
      this.rows = rows;
    }
  }

  private static class JoinAlias
  {
    final List<String> alias;
    final List<String> columns;
    final int[] indices;
    final PeekingIterator<Object[]> rows;

    private JoinAlias(List<String> alias, List<String> columns, int[] indices, Iterator<Object[]> rows)
    {
      this.alias = alias;
      this.columns = columns;
      this.indices = indices;
      this.rows = GuavaUtils.peekingIterator(rows);
    }

    private Partition next()
    {
      if (!rows.hasNext()) {
        return null;
      }
      final Object[] row = rows.peek();
      final Comparable[] joinKey = new Comparable[indices.length];
      for (int i = 0; i < joinKey.length; i++) {
        joinKey[i] = (Comparable) row[indices[i]];
      }
      return get(joinKey, Condition.EQ);
    }

    private Partition outer(final Comparable[] joinKey)
    {
      return get(joinKey, Condition.LT);
    }

    private Partition get(final Comparable[] joinKey, final Condition condition)
    {
      final List<Object[]> partition = Lists.newArrayList();
      while (rows.hasNext()) {
        final Object[] row = rows.peek();
        final int compare = compareNF(row, indices, joinKey);
        if (!condition.match(compare)) {
          break;
        }
        partition.add(rows.next());
      }
      return partition.isEmpty() && !rows.hasNext() ? null : new Partition(joinKey, partition);
    }

    private int skip(final Comparable[] joinKey)
    {
      int counter = 0;
      while (rows.hasNext()) {
        final Object[] row = rows.peek();
        final int compare = compareNF(row, indices, joinKey);
        if (!Condition.LT.match(compare)) {
          break;
        }
        rows.next();
        counter++;
      }
      return counter;
    }

    private int compareNF(Object[] row, int[] indices, Comparable[] joinKeys)
    {
      int compare = 0;
      for (int i = 0; i < indices.length && compare == 0; i++) {
        compare = Comparators.compareNF((Comparable) row[indices[i]], joinKeys[i]);
      }
      return compare;
    }
  }

  private static enum Condition
  {
    LT  { boolean match(final int compare) { return compare < 0; } },
    EQ  { boolean match(final int compare) { return compare == 0; } },
    GT  { boolean match(final int compare) { return compare > 0; } },
    ALL { boolean match(final int compare) { return true; } };

    abstract boolean match(final int compare);
  }

  private Iterator<Object[]> product(final List<Object[]> left, final List<Object[]> right)
  {
    if (left.isEmpty() || right.isEmpty()) {
      return Iterators.emptyIterator();
    }

    return new Iterator<Object[]>()
    {
      private int l;
      private int r;

      @Override
      public boolean hasNext()
      {
        return r < right.size() || l + 1 < left.size();
      }

      @Override
      public Object[] next()
      {
        if (r < right.size()) {
          return concat(left.get(l), right.get(r++));
        }
        if (l + 1 < left.size()) {
          r = 0;
          return concat(left.get(++l), right.get(r++));
        }
        throw new NoSuchElementException();
      }
    };
  }

  private Iterator<Object[]> lo(List<Object[]> left, final int right)
  {
    return Iterators.transform(
        left.iterator(), new Function<Object[], Object[]>()
        {
          @Override
          public Object[] apply(Object[] row)
          {
            return Arrays.copyOf(row, row.length + right);
          }
        }
    );
  }

  private Iterator<Object[]> ro(final int left, List<Object[]> right)
  {
    return Iterators.transform(
        right.iterator(), new Function<Object[], Object[]>()
        {
          @Override
          public Object[] apply(Object[] row)
          {
            Object[] concat = new Object[left + row.length];
            System.arraycopy(row, 0, concat, left, row.length);
            return concat;
          }
        }
    );
  }

  private static Object[] concat(Object[] left, Object[] right)
  {
    Object[] concat = Arrays.copyOf(left, left.length + right.length);
    System.arraycopy(right, 0, concat, left.length, right.length);
    return concat;
  }

  // from source.. need prefix for value
  private List<Object[]> sort(Sequence<Object[]> sequence, int[] indices)
  {
    long start = System.currentTimeMillis();
    List<Object[]> rows = Sequences.toList(sequence);
    Object[][] array = rows.toArray(new Object[rows.size()][]);
    Comparator<Object[]> comparator = Comparators.toArrayComparator(indices);
    Arrays.parallelSort(array, comparator);
    log.info(".. sorted %d rows in %,d msec", rows.size(), (System.currentTimeMillis() - start));
    return Arrays.asList(array);
  }

  private Function<Object[], Map<String, Object>> converter(List<List<String>> columnsList, List<String> aliases)
  {
    final List<String> outputColumns = Lists.newArrayList();
    for (int i = 0; i < columnsList.size(); i++) {
      List<String> columns = columnsList.get(i);
      if (aliases == null) {
        outputColumns.addAll(columns);
      } else {
        String alias = aliases.get(i) + ".";
        for (String column : columns) {
          outputColumns.add(alias + column);
        }
      }
    }
    return new Function<Object[], Map<String, Object>>()
    {
      @Override
      public Map<String, Object> apply(Object[] input)
      {
        final Map<String, Object> event = Maps.newLinkedHashMap();
        for (int i = 0; i < input.length; i++) {
          event.put(outputColumns.get(i), input[i]);
        }
        return event;
      }
    };
  }

  @Override
  public boolean hasTabularOutput()
  {
    return true;
  }
}
