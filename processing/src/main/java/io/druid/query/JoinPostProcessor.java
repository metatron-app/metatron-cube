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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.CloseableIterator;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.concurrent.PrioritizedCallable;
import io.druid.guice.annotations.Processing;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
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
public class JoinPostProcessor extends PostProcessingOperator.UnionSupport implements PostProcessingOperator.Local
{
  private static final Logger log = new Logger(JoinPostProcessor.class);

  private final JoinQueryConfig config;
  private final JoinElement[] elements;
  private final boolean prefixAlias;
  private final boolean asArray;
  private final int maxRowsInGroup;
  private final ExecutorService exec;

  @JsonCreator
  @SuppressWarnings("unchecked")
  public JoinPostProcessor(
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

  public JoinPostProcessor withAsArray(boolean asArray)
  {
    return new JoinPostProcessor(config, Arrays.asList(elements), prefixAlias, asArray, maxRowsInGroup, exec);
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
        final List<Supplier<List<OrderByColumnSpec>>> collations = Lists.newArrayList();
        final List<Sequence<Object[]>>[] sequencesList = new List[joinAliases];
        for (int i = 0; i < joinAliases; i++) {
          aliases.add(toAlias(i));
          sequencesList[i] = Lists.<Sequence<Object[]>>newArrayList();
        }
        final boolean[] nested = new boolean[joinAliases];
        final boolean[] hashing = new boolean[joinAliases];
        final MutableInt indexer = new MutableInt();
        Sequence<Pair<Query, Sequence>> sequences = baseQueryRunner.run(query, responseContext);
        sequences.accumulate(
            null, new Accumulator<Object, Pair<Query, Sequence>>()
            {
              @Override
              public Object accumulate(Object accumulated, Pair<Query, Sequence> in)
              {
                final int index = indexer.intValue();
                collations.add(getCollation(in.lhs));
                nested[index] = in.lhs instanceof JoinQuery.JoinDelegate;
                hashing[index] = in.lhs.getContextBoolean(JoinElement.HASHING, false);
                Query.ArrayOutputSupport query = (Query.ArrayOutputSupport) in.lhs;
                aliasColumnsNames.add(query.estimatedOutputColumns());
                sequencesList[index].add(query.array(in.rhs));
                indexer.increment();
                return null;
              }
            }
        );

        final Future[] joining = new Future[joinAliases];
        for (int i = 0; i < indexer.intValue(); i++) {
          // to avoid thread exhaustion, use same threded executor for nested join
          ExecutorService executor = nested[i] ? new Execs.SubmitSingleThreaded() : exec;
          joining[i] = executor.submit(
              toJoinAlias(i, hashing[i], toAlias(i), sequencesList[i], aliasColumnsNames.get(i), collations.get(i))
          );
        }
        try {
          JoinResult join = join(joining);
          if (!GuavaUtils.isNullOrEmpty(join.collation)) {
            ((JoinQuery.JoinDelegate) query).setCollation(join.collation);
          }
          Iterator outputRows = join.iterator;
          if (!asArray) {
            outputRows = GuavaUtils.map(outputRows, toMap(aliasColumnsNames, prefixAlias ? aliases : null));
          }
          return Sequences.once(outputRows);
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

  private Supplier<List<OrderByColumnSpec>> getCollation(final Query<?> query)
  {
    if (query instanceof Query.OrderingSupport) {
      return Suppliers.ofInstance(((Query.OrderingSupport<?>) query).getResultOrdering());
    } else if (query instanceof JoinQuery.JoinDelegate) {
      return new Supplier<List<OrderByColumnSpec>>()
      {
        @Override
        public List<OrderByColumnSpec> get()
        {
          return ((JoinQuery.JoinDelegate) query).getCollation();
        }
      };
    }
    return null;
  }

  private PrioritizedCallable<JoinAlias> toJoinAlias(
      final int index,
      final boolean hashing,
      final String alias,
      final List<Sequence<Object[]>> sequences,
      final List<String> columnNames,
      final Supplier<List<OrderByColumnSpec>> collation
  )
  {
    final JoinType type = toJoinType(index);
    final List<String> joinColumns = toJoinColumns(index);
    final List<String> aliases = Arrays.asList(alias);
    final int[] indices = GuavaUtils.indexOf(columnNames, joinColumns, true);
    if (indices == null) {
      throw new IAE("Cannot find join column %s in %s", joinColumns, columnNames);
    }
    if (hashing) {
      return new PrioritizedCallable.Background<JoinAlias>()
      {
        @Override
        public JoinAlias call()
        {
          final Map<Object, Object> hashed = toHashed(Sequences.toIterator(Sequences.concat(sequences)), indices);
          return new JoinAlias(aliases, columnNames, joinColumns, indices, hashed);
        }
      };
    }
    return new PrioritizedCallable.Background<JoinAlias>()
    {
      @Override
      public JoinAlias call()
      {
        final Iterator<Object[]> rows = Sequences.toIterator(Sequences.concat(sequences));
        return new JoinAlias(aliases, columnNames, joinColumns, collation, indices, rows);
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

  static Map<Object, Object> toHashed(final Iterator<Object[]> sequence, final int[] indices)
  {
    final Map<Object, Object> hashed = Maps.newHashMap();
    while (sequence.hasNext()) {
      final Object[] row = sequence.next();
      hashed.compute(JoinKey.hashKey(row, indices), new BiFunction<Object, Object, Object>()
      {
        @Override
        @SuppressWarnings("unchecked")
        public Object apply(Object key, Object prev)
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
    }
    return hashed;
  }

  @VisibleForTesting
  final JoinResult join(final Future<JoinAlias>[] futures) throws Exception
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
          GuavaUtils.indexOf(columns, joinColumns), result.iterator
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
  final JoinResult join(JoinAlias left, JoinAlias right, final int index)
  {
    Preconditions.checkArgument(left.joinColumns.size() == right.joinColumns.size());
    if (left.joinColumns.size() == 0) {
      log.info("... start cross join %s to %s", left, right);
      return JoinResult.of(product(left.materialize(), right.materialize(), false));
    }
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
    } else if (type.isLeftDrivable() && right.isHashed()) {
      return joinHashed(left, right, type, true);
    } else if (type.isRightDrivable() && left.isHashed()) {
      return joinHashed(left, right, type, false);
    }
    if (!left.isSorted()) {
      left = left.transform(type.isRightDrivable() ? config.getHashJoinThreshold() : -1);
      if (left.isHashed()) {
        return joinHashed(left, right, type, false);
      }
      Preconditions.checkArgument(left.isSorted());
    }
    if (!right.isSorted()) {
      right = right.transform(type.isLeftDrivable() ? config.getHashJoinThreshold() : -1);
      if (right.isHashed()) {
        return joinHashed(left, right, type, true);
      }
      Preconditions.checkArgument(right.isSorted());
    }
    log.info("... start %s join %s to %s (%s)", type, left, right, "SortedMerge");

    final List<OrderByColumnSpec> collation = OrderByColumnSpec.ascending(left.joinColumns);
    return JoinResult.of(collation, new JoinIterator(type, left, right)
    {
      @Override
      protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
      {
        return mergeJoin(type, leftAlias, rightAlias);
      }
    });
  }

  private JoinResult joinHashed(
      final JoinAlias left,
      final JoinAlias right,
      final JoinType type,
      final boolean leftDriving
  )
  {
    log.info("... start %s join %s %s %s", type, left, leftDriving ? "-->" : "<--", right);
    if (leftDriving) {
      if (left.isHashed()) {
        return JoinResult.of(new JoinIterator(type, left.prepareHashIterator(), right)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return hashedHashJoin(type, leftAlias, rightAlias, false);
          }
        });
      }
      if (left.isSorted()) {
        List<OrderByColumnSpec> collation = OrderByColumnSpec.ascending(left.joinColumns);
        return JoinResult.of(collation, new JoinIterator(type, left, right)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return hashJoinPartitioned(type, leftAlias, rightAlias, false);
          }
        });
      }
      return JoinResult.of(new JoinIterator(type, left, right)
      {
        @Override
        protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
        {
          return hashJoin(type, leftAlias, rightAlias, false);
        }
      });
    } else {
      if (right.isHashed()) {
        return JoinResult.of(new JoinIterator(type, left, right.prepareHashIterator())
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return hashedHashJoin(type.revert(), rightAlias, leftAlias, true);
          }
        });
      }
      if (right.isSorted()) {
        List<OrderByColumnSpec> collation = OrderByColumnSpec.ascending(right.joinColumns);
        return JoinResult.of(collation, new JoinIterator(type, left, right)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return hashJoinPartitioned(type.revert(), rightAlias, leftAlias, true);
          }
        });
      }
      return JoinResult.of(new JoinIterator(type, left, right)
      {
        @Override
        protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
        {
          return hashJoin(type.revert(), rightAlias, leftAlias, true);
        }
      });
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
      final int compare = compareNF(left.partition.get(0), left.indices, right.partition.get(0), right.indices);
      if (compare == 0) {
        Iterator<Object[]> product = product(left.partition, right.partition, false);
        left.partition = left.next();
        right.partition = right.next();
        return product;
      }
      switch (type) {
        case INNER:
          if (compare < 0) {
            left.partition = left.skip(right.partition.get(0), right.indices);
          } else {
            right.partition = right.skip(left.partition.get(0), left.indices);
          }
          continue;
        case LO:
          if (compare < 0) {
            Iterator<Object[]> lo = lo(left.partition.iterator(), right.columns.size());
            left.partition = left.next();
            return lo;
          } else {
            right.partition = right.skip(left.partition.get(0), left.indices);
          }
          continue;
        case RO:
          if (compare < 0) {
            left.partition = left.skip(right.partition.get(0), right.indices);
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

  private static int compareNF(final Object[] row1, final Object[] row2, final int[] indices)
  {
    int compare = 0;
    for (int i = 0; i < indices.length && compare == 0; i++) {
      compare = Comparators.compareNF((Comparable) row1[indices[i]], (Comparable) row2[indices[i]]);
    }
    return compare;
  }

  private static int compareNF(final Object[] row1, final int[] indices1, final Object[] row2, final int[] indices2)
  {
    int compare = 0;
    for (int i = 0; i < indices1.length && compare == 0; i++) {
      compare = Comparators.compareNF((Comparable) row1[indices1[i]], (Comparable) row2[indices2[i]]);
    }
    return compare;
  }

  private Iterator<Object[]> hashJoinPartitioned(
      final JoinType type,
      final JoinAlias left,
      final JoinAlias right,
      final boolean revert
  )
  {
    for (left.partition = left.next(); left.partition != null; left.partition = left.next()) {
      final List<Object[]> rightRows = right.getHashed(JoinKey.hashKey(left.partition.get(0), left.indices));
      if (rightRows != null) {
        return product(left.partition, rightRows, revert);
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
      final Map.Entry<Object, Object> entry = driving.iterator.next();
      final List<Object[]> leftRows = asValues(entry.getValue());
      final List<Object[]> rightRows = target.getHashed(entry.getKey());
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
      final Object[] drivingRow = driving.rows.next();
      final List<Object[]> otherRows = target.getHashed(JoinKey.hashKey(drivingRow, driving.indices));
      if (otherRows != null) {
        return product(Arrays.<Object[]>asList(drivingRow), otherRows, revert);
      }
      if (type == JoinType.LO) {
        return Iterators.singletonIterator(Arrays.copyOf(drivingRow, drivingRow.length + target.columns.size()));
      }
    }
    return null;
  }

  private static class JoinKey implements Comparable<JoinKey>
  {
    static Object hashKey(Object[] row, int[] indices)
    {
      if (indices.length == 1) {
        return row[indices[0]];
      }
      final Object[] joinKey = new Object[indices.length];
      for (int i = 0; i < indices.length; i++) {
        joinKey[i] = row[indices[i]];
      }
      return new JoinKey(joinKey);
    }

    private final Object[] joinKey;

    private JoinKey(Object[] joinKey) {this.joinKey = joinKey;}

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
        compare = Comparators.compareNF((Comparable) joinKey[i], (Comparable) o.joinKey[i]);
      }
      return compare;
    }
  }

  static final class JoinAlias implements Closeable
  {
    final List<String> alias;
    final List<String> columns;
    final List<String> joinColumns;
    final Supplier<List<OrderByColumnSpec>> collation;  // set after inner query is executed
    final int[] indices;
    final PeekingIterator<Object[]> rows;
    final Map<Object, Object> hashed;

    Iterator<Map.Entry<Object, Object>> iterator;  // hash iterator
    List<Object[]> partition;

    JoinAlias(
        List<String> alias,
        List<String> columns,
        List<String> joinColumns,
        Supplier<List<OrderByColumnSpec>> collation,
        int[] indices,
        Iterator<Object[]> rows
    )
    {
      log.info("---> %s = stream", alias);
      this.alias = alias;
      this.columns = columns;
      this.joinColumns = joinColumns;
      this.collation = collation;
      this.indices = indices;
      this.rows = GuavaUtils.peekingIterator(rows);
      this.hashed = null;
    }

    JoinAlias(
        List<String> alias,
        List<String> columns,
        List<String> joinColumns,
        int[] indices,
        Map<Object, Object> hashed
    )
    {
      log.info("---> %s = hashed (group=%d)", alias, hashed.size());
      this.alias = alias;
      this.columns = columns;
      this.joinColumns = joinColumns;
      this.collation = null;
      this.indices = indices;
      this.rows = Iterators.peekingIterator(Iterators.<Object[]>emptyIterator());
      this.hashed = hashed;
    }

    private boolean isHashed()
    {
      return hashed != null;
    }

    private boolean isSorted()
    {
      // todo: regarded as ascending
      return collation != null && joinColumns.equals(OrderByColumnSpec.getColumns(collation.get()));
    }

    private JoinAlias prepareHashIterator()
    {
      iterator = hashed.entrySet().iterator();
      return this;
    }

    private List<Object[]> materialize()
    {
      List<Object[]> materialized;
      if (hashed != null) {
        materialized = Lists.<Object[]>newArrayList(
            Iterables.concat(
                Iterables.transform(
                    hashed.values(),
                    new Function<Object, List<Object[]>>()
                    {
                      @Override
                      public List<Object[]> apply(Object input)
                      {
                        return asValues(input);
                      }
                    }
                )
            )
        );
        hashed.clear();
      } else {
        materialized = Lists.<Object[]>newArrayList(rows);
      }
      return materialized;
    }

    private JoinAlias transform(int hashThreshold)
    {
      final List<Object[]> materialized = Sequences.toList(Sequences.once(rows));
      if (hashThreshold > 0 && materialized.size() < hashThreshold) {
        return new JoinAlias(alias, columns, joinColumns, indices, toHashed(materialized.iterator(), indices));
      }
      final List<Object[]> sort = sort(alias, materialized, indices);
      final List<OrderByColumnSpec> collation = OrderByColumnSpec.ascending(joinColumns);
      return new JoinAlias(alias, columns, joinColumns, Suppliers.ofInstance(collation), indices, sort.iterator());
    }

    // not empty
    private List<Object[]> next()
    {
      if (!rows.hasNext()) {
        return null;
      }
      final Object[] current = rows.next();
      if (!rows.hasNext() || compareNF(current, rows.peek(), indices) != 0) {
        return Arrays.<Object[]>asList(current);
      }
      final List<Object[]> partition = Lists.<Object[]>newArrayList(current, rows.next());
      while (rows.hasNext()) {
        if (compareNF(current, rows.peek(), indices) != 0) {
          break;
        }
        partition.add(rows.next());
      }
      return partition;
    }

    private List<Object[]> skip(final Object[] row2, final int[] indices2)
    {
      for (; rows.hasNext(); rows.next()) {
        if (compareNF(rows.peek(), indices, row2, indices2) >= 0) {
          break;
        }
      }
      return next();
    }

    private List<Object[]> getHashed(Object joinKey)
    {
      return asValues(hashed.get(joinKey));
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

    @Override
    public String toString()
    {
      return alias + "." + joinColumns + (isHashed() ? "(hashed)" : isSorted() ? "(sorted-stream)" : "");
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
  private static List<Object[]> sort(List<String> alias, List<Object[]> rows, int[] indices)
  {
    long start = System.currentTimeMillis();
    Object[][] array = rows.toArray(new Object[rows.size()][]);
    Comparator<Object[]> comparator = Comparators.toArrayComparator(indices);
    Arrays.parallelSort(array, comparator);
    log.info(".. %s sorted %d rows in %,d msec", alias, rows.size(), (System.currentTimeMillis() - start));
    return Arrays.asList(array);
  }

  private Function<Object[], Map<String, Object>> toMap(List<List<String>> columnsList, List<String> aliases)
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

  static class JoinResult
  {
    static JoinResult of(Iterator<Object[]> iterator)
    {
      return new JoinResult(iterator, null);
    }

    static JoinResult of(List<OrderByColumnSpec> collation, Iterator<Object[]> iterator)
    {
      return new JoinResult(iterator, collation);
    }

    final Iterator<Object[]> iterator;
    final List<OrderByColumnSpec> collation;

    private JoinResult(Iterator<Object[]> iterator, List<OrderByColumnSpec> collation)
    {
      this.iterator = iterator;
      this.collation = collation;
    }
  }

}
