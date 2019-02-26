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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.CloseableIterator;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.PrioritizedCallable;
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
import java.util.function.BiFunction;

/**
 */
public class XJoinPostProcessor extends PostProcessingOperator.UnionSupport implements PostProcessingOperator.Local
{
  private static final Logger log = new Logger(XJoinPostProcessor.class);

  private final JoinQueryConfig config;
  private final JoinElement[] elements;
  private final boolean prefixAlias;
  private final boolean asArray;
  private final int maxRowsInGroup;
  private final ExecutorService exec;

  @JsonCreator
  @SuppressWarnings("unchecked")
  public XJoinPostProcessor(
      @JacksonInject JoinQueryConfig config,
      @JsonProperty("elements") List<JoinElement> elements,
      @JsonProperty("prefixAlias") boolean prefixAlias,
      @JsonProperty("asArray") boolean asArray,
      @JsonProperty("maxRowsInGroup") int maxRowsInGroup,
      @JacksonInject @Processing ExecutorService exec
  )
  {
    this.config = config;
    this.elements = elements.toArray(new JoinElement[0]);
    this.asArray = asArray;
    this.maxRowsInGroup = Math.min(config.getMaxRowsInGroup(), maxRowsInGroup);
    this.prefixAlias = prefixAlias;
    this.exec = exec;
  }

  public boolean asArray()
  {
    return asArray;
  }

  public XJoinPostProcessor withAsArray(boolean asArray)
  {
    return new XJoinPostProcessor(config, Arrays.asList(elements), prefixAlias, asArray, maxRowsInGroup, exec);
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
        final boolean[] hashing = new boolean[joinAliases];
        final MutableInt indexer = new MutableInt();
        Sequence<Pair<Query, Sequence>> sequences = baseQueryRunner.run(query, responseContext);
        sequences.accumulate(
            null, new Accumulator<Object, Pair<Query, Sequence>>()
            {
              @Override
              public Object accumulate(Object accumulated, Pair<Query, Sequence> in)
              {
                sorted[indexer.intValue()] = in.lhs.getContextBoolean(JoinElement.SORTED_ON_JOINKEY, false);
                hashing[indexer.intValue()] = in.lhs.getContextBoolean(JoinElement.HASHABLE, false);
                Query.ArrayOutputSupport query = (Query.ArrayOutputSupport) in.lhs;
                aliasColumnsNames.add(query.estimatedOutputColumns());
                sequencesList[indexer.intValue()].add(query.array(in.rhs));
                indexer.increment();
                return null;
              }
            }
        );

        final Future[] joining = new Future[joinAliases];
        for (int i = 0; i < indexer.intValue(); i++) {
          joining[i] = exec.submit(
              toJoinAlias(
                  i,
                  sorted[i],
                  hashing[i],
                  toAlias(i),
                  sequencesList[i],
                  aliasColumnsNames.get(i)
              )
          );
        }
        try {
          Iterator join = join(joining);
          if (!asArray) {
            join = GuavaUtils.map(join, converter(aliasColumnsNames, prefixAlias ? aliases : null));
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

  private PrioritizedCallable<JoinAlias> toJoinAlias(
      final int index,
      final boolean sorted,
      final boolean hashing,
      final String alias,
      final List<Sequence<Object[]>> sequences,
      final List<String> columnNames
  )
  {
    final JoinType type = toJoinType(index);
    final List<String> joinColumns = toJoinColumns(index);
    final List<String> aliases = Arrays.asList(alias);
    final int[] indices = GuavaUtils.indexOf(columnNames, joinColumns);
    if (hashing) {
      return new PrioritizedCallable.Background<JoinAlias>()
      {
        @Override
        public JoinAlias call()
        {
          final Map<JoinKey, Object> hashed = toHashed(Sequences.concat(sequences), indices, sorted);
          return new JoinAlias(aliases, columnNames, joinColumns, indices, hashed);
        }
      };
    }
    return new PrioritizedCallable.Background<JoinAlias>()
    {
      @Override
      public JoinAlias call()
      {
        final Sequence<Object[]> sequence = Sequences.concat(sequences);
        if (sorted) {
          return new JoinAlias(aliases, columnNames, joinColumns, indices, Sequences.toIterator(sequence), true);
        }
        final int threshold = config.getHashJoinThreshold();
        final List<Object[]> rows = Sequences.toList(sequence);
        if (threshold > 0 && rows.size() < threshold &&
            (index == 0 && type.isRightDriving() || index > 0 && type.isLeftDriving())) {
          final Map<JoinKey, Object> hashed = toHashed(Sequences.simple(rows), indices, false);
          return new JoinAlias(aliases, columnNames, joinColumns, indices, hashed);
        }
        return new JoinAlias(aliases, columnNames, joinColumns, indices, sort(rows, indices).iterator(), true);
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

  static Map<JoinKey, Object> toHashed(final Sequence<Object[]> sequence, final int[] indices, final boolean sorted)
  {
    final Map<JoinKey, Object> hashed = Maps.newHashMap();

    sequence.accumulate(null, new Accumulator<Object, Object[]>()
    {
      @Override
      public Object accumulate(Object accumulated, final Object[] row)
      {
        final Comparable[] joinKey = new Comparable[indices.length];
        for (int i = 0; i < joinKey.length; i++) {
          joinKey[i] = (Comparable) row[indices[i]];
        }
        hashed.compute(new JoinKey(joinKey), new BiFunction<JoinKey, Object, Object>()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Object apply(JoinKey key, Object prev)
          {
            if (prev == null) {
              return row;
            }
            if (prev instanceof List) {
              ((List) prev).add(row);
              return prev;
            }
            return Lists.newArrayList(prev, row);
          }
        });
        return null;
      }
    });
    return hashed;
  }

  @VisibleForTesting
  final Iterator<Object[]> join(final Future<JoinAlias>[] futures) throws Exception
  {
    JoinAlias left = futures[0].get();
    JoinAlias right = futures[1].get();
    Iterator<Object[]> iterator = join(left, right, 0);
    List<String> alias = GuavaUtils.concat(left.alias, right.alias);
    List<String> columns = GuavaUtils.concat(left.columns, right.columns);
    List<String> joinColumns = elements[0].getLeftJoinColumns();
    for (int i = 2; i < futures.length; i++) {
      right = futures[i].get();
      final int[] indices = GuavaUtils.indexOf(columns, joinColumns);
      boolean sorted;
      Iterator<Object[]> leftRows;
      if (right.isHashed() || joinColumns.equals(left.joinColumns) || joinColumns.equals(right.joinColumns)) {
        leftRows = iterator;  // no need to sort
        sorted = !right.isHashed();
      } else {
        // not Lists.newArrayList() for closing resources
        leftRows = sort(Sequences.toList(Sequences.once(iterator)), indices).iterator();
        sorted = true;
      }
      left = new JoinAlias(alias, columns, joinColumns, indices, leftRows, sorted);
      iterator = join(left, right, 0);
      alias = GuavaUtils.concat(alias, right.alias);
      columns = GuavaUtils.concat(columns, right.columns);
      joinColumns = elements[i - 1].getLeftJoinColumns();
    }
    return iterator;
  }

  @VisibleForTesting
  final Iterator<Object[]> join(JoinAlias left, JoinAlias right, final int index)
  {
    final JoinType type = elements[index].getJoinType();
    if (left.isHashed() && right.isHashed()) {
      switch (type) {
        case INNER:
          boolean leftDriving = left.hashed.size() < right.hashed.size();
          return joinHashed(left, right, type, leftDriving);
        case LO:
          return joinHashed(left, right, type, true);
        case RO:
          return joinHashed(left, right, type, false);
        default:
          throw new ISE("Cannot %s join with hash", type);
      }
    } else if (type.isLeftDriving() && right.isHashed()) {
      return joinHashed(left, right, type, true);
    } else if (type.isRightDriving() && left.isHashed()) {
      return joinHashed(left, right, type, false);
    }
    log.info("... start %s join %s to %s (%s)", type, left.alias, right.alias, "SortedMerge");
    assert left.isSorted() && right.isSorted();
    return new JoinIterator(type, left, right)
    {
      @Override
      protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
      {
        return mergeJoin(type, leftAlias, rightAlias);
      }
    };
  }

  private Iterator<Object[]> joinHashed(
      final JoinAlias left,
      final JoinAlias right,
      final JoinType type,
      final boolean leftDriving
  )
  {
    log.info(
        "... start %s join %s%s and %s%s",
        type, left.alias, leftDriving ? "" : "(hashed)", right.alias, leftDriving ? "(hashed)" : ""
    );
    if (leftDriving) {
      if (left.isHashed()) {
        left.prepareHashIterator();
        return new JoinIterator(type, left, right)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return hashedHashJoin(type, leftAlias, rightAlias, false);
          }
        };
      }
      if (left.isSorted()) {
        return new JoinIterator(type, left, right)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return hashJoinPartitioned(type, leftAlias, rightAlias, false);
          }
        };
      }
      return new JoinIterator(type, left, right)
      {
        @Override
        protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
        {
          return hashJoin(type, leftAlias, rightAlias, false);
        }
      };
    } else {
      if (right.isHashed()) {
        right.prepareHashIterator();
        return new JoinIterator(type, left, right)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return hashedHashJoin(type.revert(), rightAlias, leftAlias, true);
          }
        };
      }
      if (left.isSorted()) {
        return new JoinIterator(type, left, right)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return hashJoinPartitioned(type.revert(), rightAlias, leftAlias, true);
          }
        };
      }
      return new JoinIterator(type, left, right)
      {
        @Override
        protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
        {
          return hashJoin(type.revert(), rightAlias, leftAlias, true);
        }
      };
    }
  }

  private static abstract class JoinIterator implements CloseableIterator<Object[]>
  {
    final JoinType type;
    final JoinAlias leftAlias;
    final JoinAlias rightAlias;

    protected JoinIterator(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
    {
      this.type = type;
      this.leftAlias = leftAlias;
      this.rightAlias = rightAlias;
    }

    @Override
    public void close() throws IOException
    {
      leftAlias.close();
      rightAlias.close();
    }

    private Iterator<Object[]> iterator = Iterators.emptyIterator();

    @Override
    public boolean hasNext()
    {
      while (iterator != null && !iterator.hasNext()) {
        iterator = next(type, leftAlias, rightAlias);
      }
      return iterator != null;
    }

    protected abstract Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias);

    @Override
    public Object[] next()
    {
      return iterator.next();
    }
  }

  private Iterator<Object[]> mergeJoin(final JoinType type, final JoinAlias left, final JoinAlias right)
  {
    if (left.partition == null) {
      left.partition = left.next();
    }
    if (right.partition == null) {
      right.partition = right.next();
    }
    while (left.partition != null && right.partition != null) {
      final int compare = Comparators.compareNF(left.partition.joinKey, right.partition.joinKey);
      if (compare == 0) {
        Iterator<Object[]> product = product(left.partition.rows, right.partition.rows, false);
        left.partition = left.next();
        right.partition = right.next();
        return product;
      }
      switch (type) {
        case INNER:
          if (compare < 0) {
            left.partition = left.skip(right.partition.joinKey);
          } else {
            right.partition = right.skip(left.partition.joinKey);
          }
          continue;
        case LO:
          if (compare < 0) {
            Iterator<Object[]> lo = lo(left.partition.iterator(), right.columns.size());
            left.partition = left.next();
            return lo;
          } else {
            right.partition = right.skip(left.partition.joinKey);
          }
          continue;
        case RO:
          if (compare < 0) {
            left.partition = left.skip(right.partition.joinKey);
          } else {
            Iterator<Object[]> ro = ro(left.columns.size(), right.partition.iterator());
            right.partition = right.next();
            return ro;
          }
          continue;
        case FULL:
          if (compare < 0) {
            Iterator<Object[]> lo = lo(left.partition.iterator(), right.columns.size());
            left.partition = left.next();
            return lo;
          } else {
            Iterator<Object[]> ro = ro(left.columns.size(), right.partition.iterator());
            right.partition = right.next();
            return ro;
          }
        default:
          throw new UnsupportedOperationException("not supported type " + type);
      }
    }
    if (left.partition != null && (type == JoinType.LO || type == JoinType.FULL)) {
      Iterator<Object[]> lo = lo(Iterators.concat(left.partition.iterator(), left.rows), right.columns.size());
      left.partition = null;
      return lo;
    } else if (right.partition != null && (type == JoinType.RO || type == JoinType.FULL)) {
      Iterator<Object[]> ro = ro(left.columns.size(), Iterators.concat(right.partition.iterator(), right.rows));
      right.partition = null;
      return ro;
    }
    left.partition = null;
    right.partition = null;
    return null;
  }

  private Iterator<Object[]> hashJoinPartitioned(
      final JoinType type,
      final JoinAlias left,
      final JoinAlias right,
      final boolean revert
  )
  {
    for (left.partition = left.next(); left.partition != null; left.partition = left.next()) {
      final List<Object[]> rightRows = right.getHashed(left.partition.joinKey);
      if (rightRows != null) {
        return product(left.partition.rows, rightRows, revert);
      }
      if (type == JoinType.LO) {
        return revert ? ro(right.columns.size(), left.partition.iterator())
                      : lo(left.partition.iterator(), right.columns.size());
      }
    }
    return null;
  }

  private Iterator<Object[]> hashedHashJoin(
      final JoinType type,
      final JoinAlias driving,
      final JoinAlias target,
      final boolean revert
  )
  {
    while (driving.iterator.hasNext()) {
      final Map.Entry<JoinKey, Object> entry = driving.iterator.next();
      final List<Object[]> leftRows = asValues(entry.getValue());
      final List<Object[]> rightRows = target.getHashed(entry.getKey().joinKey);
      if (rightRows != null) {
        return product(leftRows, rightRows, revert);
      }
      if (type == JoinType.LO) {
        return revert ? ro(driving.columns.size() + target.columns.size(), leftRows.iterator())
               : lo(leftRows.iterator(), driving.columns.size() + target.columns.size());
      }
    }
    return null;
  }

  private Iterator<Object[]> hashJoin(
      final JoinType type,
      final JoinAlias driving,
      final JoinAlias target,
      final boolean revert
  )
  {
    while (driving.rows.hasNext()) {
      final Object[] leftRow = driving.rows.next();
      final List<Object[]> rightRows = target.getHashed(driving.asJoinKey(leftRow));
      if (rightRows != null) {
        return product(Arrays.<Object[]>asList(leftRow), rightRows, revert);
      }
      if (type == JoinType.LO) {
        return Iterators.singletonIterator(Arrays.copyOf(leftRow, leftRow.length + target.columns.size()));
      }
    }
    return null;
  }

  private static class Partition implements Iterable<Object[]>
  {
    final Comparable[] joinKey;
    final List<Object[]> rows;

    private Partition(Comparable[] joinKey, List<Object[]> rows)
    {
      this.joinKey = joinKey;
      this.rows = rows;
    }

    @Override
    public Iterator<Object[]> iterator()
    {
      return rows.iterator();
    }
  }

  private static class JoinKey implements Comparable<JoinKey>
  {
    private final Comparable[] joinKey;

    private JoinKey(Comparable[] joinKey) {this.joinKey = joinKey;}

    @Override
    public int hashCode()
    {
      return Arrays.hashCode(joinKey);
    }

    @Override
    public boolean equals(Object other)
    {
      return Arrays.equals(joinKey, ((JoinKey) other).joinKey);
    }

    @Override
    public int compareTo(JoinKey o)
    {
      int compare = 0;
      for (int i = 0; compare == 0 && i < joinKey.length; i++) {
        compare = Comparators.compareNF(joinKey[0], o.joinKey[i]);
      }
      return compare;
    }
  }

  static final class JoinAlias implements Closeable
  {
    final List<String> alias;
    final List<String> columns;
    final List<String> joinColumns;
    final int[] indices;
    final PeekingIterator<Object[]> rows;
    final Map<JoinKey, Object> hashed;
    final boolean sorted;

    Iterator<Map.Entry<JoinKey, Object>> iterator;
    Partition partition;

    JoinAlias(
        List<String> alias,
        List<String> columns,
        List<String> joinColumns,
        int[] indices,
        Iterator<Object[]> rows,
        boolean sorted
    )
    {
      log.info("----> %s = SMB (sorted=%s)", alias, sorted);
      this.alias = alias;
      this.columns = columns;
      this.joinColumns = joinColumns;
      this.indices = indices;
      this.rows = GuavaUtils.peekingIterator(rows);
      this.hashed = null;
      this.sorted = sorted;
    }

    JoinAlias(
        List<String> alias,
        List<String> columns,
        List<String> joinColumns,
        int[] indices,
        Map<JoinKey, Object> hashed
    )
    {
      log.info("----> %s = hashed (%d)", alias, hashed.size());
      this.alias = alias;
      this.columns = columns;
      this.joinColumns = joinColumns;
      this.indices = indices;
      this.rows = Iterators.peekingIterator(Iterators.<Object[]>emptyIterator());
      this.hashed = hashed;
      this.sorted = false;
    }

    private boolean isHashed()
    {
      return hashed != null;
    }

    private boolean isSorted()
    {
      return sorted;
    }

    private void prepareHashIterator()
    {
      iterator = hashed.entrySet().iterator();
    }

    private Partition next()
    {
      if (!rows.hasNext()) {
        return null;
      }
      final Object[] current = rows.next();
      final Comparable[] joinKey = asJoinKey(current);
      if (!rows.hasNext() || compareNF(rows.peek(), joinKey) != 0) {
        return new Partition(joinKey, Arrays.<Object[]>asList(current));
      }
      final List<Object[]> partition = Lists.<Object[]>newArrayList(current, rows.next());
      while (rows.hasNext()) {
        if (compareNF(rows.peek(), joinKey) != 0) {
          break;
        }
        partition.add(rows.next());
      }
      return new Partition(joinKey, partition);
    }

    private Comparable[] asJoinKey(final Object[] current)
    {
      final Comparable[] joinKey = new Comparable[indices.length];
      for (int i = 0; i < joinKey.length; i++) {
        joinKey[i] = (Comparable) current[indices[i]];
      }
      return joinKey;
    }

    private Partition skip(final Comparable[] joinKey)
    {
      for (; rows.hasNext(); rows.next()) {
        if (compareNF(rows.peek(), joinKey) >= 0) {
          break;
        }
      }
      return next();
    }

    private int compareNF(final Object[] row, final Comparable[] joinKeys)
    {
      int compare = 0;
      for (int i = 0; i < indices.length && compare == 0; i++) {
        compare = Comparators.compareNF((Comparable) row[indices[i]], joinKeys[i]);
      }
      return compare;
    }

    private List<Object[]> getHashed(Comparable[] joinKey)
    {
      return asValues(hashed.get(new JoinKey(joinKey)));
    }

    @Override
    public void close() throws IOException
    {
      if (rows instanceof Closeable) {
        ((Closeable) rows).close();
      }
      if (hashed != null) {
        hashed.clear();
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Object[]> asValues(Object value)
  {
    if (value == null || value instanceof List) {
      return ((List<Object[]>) value);
    } else {
      return Arrays.<Object[]>asList((Object[]) value);
    }
  }

  private Iterator<Object[]> product(final List<Object[]> left, final List<Object[]> right, final boolean revert)
  {
    if (left.isEmpty() || right.isEmpty()) {
      return Iterators.emptyIterator();
    }
    if (maxRowsInGroup > 0 && left.size() * right.size() > maxRowsInGroup) {
      throw new ISE("Exceeding max number of single group %d, %d", maxRowsInGroup, left.size() * right.size());
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
          return concat(left.get(l), right.get(r++), revert);
        }
        if (l + 1 < left.size()) {
          r = 0;
          return concat(left.get(++l), right.get(r++), revert);
        }
        throw new NoSuchElementException();
      }
    };
  }

  private Iterator<Object[]> lo(final Iterator<Object[]> left, final int right)
  {
    return Iterators.transform(
        left, new Function<Object[], Object[]>()
        {
          @Override
          public Object[] apply(Object[] row)
          {
            return Arrays.copyOf(row, row.length + right);
          }
        }
    );
  }

  private Iterator<Object[]> ro(final int left, final Iterator<Object[]> right)
  {
    return Iterators.transform(
        right, new Function<Object[], Object[]>()
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

  private static Object[] concat(final Object[] left, final Object[] right, final boolean revert)
  {
    if (revert) {
      Object[] concat = Arrays.copyOf(right, left.length + right.length);
      System.arraycopy(left, 0, concat, right.length, left.length);
      return concat;
    } else {
      Object[] concat = Arrays.copyOf(left, left.length + right.length);
      System.arraycopy(right, 0, concat, left.length, right.length);
      return concat;
    }
  }

  // from source.. need prefix for value
  private List<Object[]> sort(List<Object[]> rows, int[] indices)
  {
    long start = System.currentTimeMillis();
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
    return !asArray;
  }
}
