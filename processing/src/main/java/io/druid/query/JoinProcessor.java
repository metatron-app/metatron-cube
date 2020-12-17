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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import io.druid.common.guava.Comparators;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.query.Query.OrderingSupport;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiFunction;

public class JoinProcessor
{
  public static final Supplier<List<List<OrderByColumnSpec>>> NO_COLLATION = Suppliers.ofInstance(ImmutableList.of());

  protected final Logger LOG = new Logger(getClass());

  protected final JoinQueryConfig config;
  protected final int maxOutputRow;

  public JoinProcessor(JoinQueryConfig config, int maxOutputRow)
  {
    this.config = config;
    this.maxOutputRow = config == null ? maxOutputRow : config.getMaxOutputRow(maxOutputRow);
  }

  @VisibleForTesting
  final JoinResult join(JoinType type, JoinAlias left, JoinAlias right)
  {
    Preconditions.checkArgument(left.joinColumns.size() == right.joinColumns.size());
    if (left.joinColumns.size() == 0) {
      LOG.info(">> CROSS (%s x %s)", left, right);
      return JoinResult.none(product(left.materialize(), right.materialize(), false));
    }
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
    if (type.isLeftDrivable() && !right.isSorted() && right.estimatedNumRows > 0) {
      JoinAlias hashed = right.tryHashOnEstimation(config.getHashJoinThreshold());
      if (hashed != null) {
        return joinHashed(left, hashed, type, true);
      }
    }
    if (type.isRightDrivable() && !left.isSorted() && left.estimatedNumRows > 0) {
      JoinAlias hashed = left.tryHashOnEstimation(config.getHashJoinThreshold());
      if (hashed != null) {
        return joinHashed(hashed, right, type, false);
      }
    }
    if (left.estimatedNumRows > 0 && right.estimatedNumRows > 0) {
      if (type.isLeftDrivable()
          && left.isSorted()
          && !right.isSorted()
          && left.estimatedNumRows > right.estimatedNumRows) {
        right = right.sortAll();
      } else if (type.isRightDrivable()
                 && !left.isSorted()
                 && right.isSorted()
                 && left.estimatedNumRows < right.estimatedNumRows) {
        left = left.sortAll();
      }
    }
    if (!left.isSorted()) {
      left = left.hashOrSort(type.isRightDrivable() ? config.getHashJoinThreshold() : -1);
      if (left.isHashed()) {
        return joinHashed(left, right, type, false);
      }
      Preconditions.checkArgument(left.isSorted());
    }
    if (!right.isSorted()) {
      right = right.hashOrSort(type.isLeftDrivable() ? config.getHashJoinThreshold() : -1);
      if (right.isHashed()) {
        return joinHashed(left, right, type, true);
      }
      Preconditions.checkArgument(right.isSorted());
    }
    return joinSorted(left, right, type);
  }

  private JoinResult joinSorted(JoinAlias left, JoinAlias right, JoinType type)
  {
    LOG.info(">> %s (%s --> %s) (SortedMerge)", type, left, right);
    return JoinResult.sortMerge(new JoinIterator(type, left, right, maxOutputRow)
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
    LOG.info(">> %s (%s %s %s) (%s + %s)", type, left, leftDriving ? "-->" : "<--", right, left.columns, right.columns);
    if (leftDriving) {
      if (left.isHashed()) {
        return JoinResult.none(new JoinIterator(type, left.prepareHashIterator(), right, maxOutputRow)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return hashToHashJoin(type, leftAlias, rightAlias, false);
          }
        });
      }
      if (left.isSorted()) {
        return JoinResult.left(new JoinIterator(type, left, right, maxOutputRow)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return sortedToHashJoin(type, leftAlias, rightAlias, false);
          }
        });
      }
      return JoinResult.none(new JoinIterator(type, left, right, maxOutputRow)
      {
        @Override
        protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
        {
          return hashJoin(type, leftAlias, rightAlias, false);
        }
      });
    } else {
      if (right.isHashed()) {
        return JoinResult.none(new JoinIterator(type, left, right.prepareHashIterator(), maxOutputRow)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return hashToHashJoin(type.revert(), rightAlias, leftAlias, true);
          }
        });
      }
      if (right.isSorted()) {
        return JoinResult.right(new JoinIterator(type, left, right, maxOutputRow)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return sortedToHashJoin(type.revert(), rightAlias, leftAlias, true);
          }
        });
      }
      return JoinResult.none(new JoinIterator(type, left, right, maxOutputRow)
      {
        @Override
        protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
        {
          return hashJoin(type.revert(), rightAlias, leftAlias, true);
        }
      });
    }
  }

  private abstract class JoinIterator implements CloseableIterator<Object[]>
  {
    final JoinType type;
    final JoinAlias leftAlias;
    final JoinAlias rightAlias;
    final int limit;

    protected JoinIterator(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias, int limit)
    {
      this.type = type;
      this.leftAlias = leftAlias;
      this.rightAlias = rightAlias;
      this.limit = limit;
    }

    private List<List<OrderByColumnSpec>> asCollations()
    {
      switch (type) {
        case INNER:
          return Arrays.asList(leftAlias.asCollation(), rightAlias.asCollation());
        case LO:
          return Arrays.asList(leftAlias.asCollation());
        case RO:
          return Arrays.asList(rightAlias.asCollation());
      }
      return ImmutableList.of();
    }

    @Override
    public void close() throws IOException
    {
      LOG.info(
          "<< %s (%s + %s), resulting %d rows (%s+%s)",
          type,
          leftAlias,
          rightAlias,
          count,
          leftAlias.columns,
          rightAlias.columns
      );
      leftAlias.close();
      rightAlias.close();
    }

    private int count;
    private Iterator<Object[]> iterator = Collections.emptyIterator();

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
      if (limit > 0 && ++count > limit) {
        throw new ISE("Exceeding maxOutputRow of %d in %s + %s", limit, leftAlias.alias, rightAlias.alias);
      }
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

  private Iterator<Object[]> sortedToHashJoin(
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

  private Iterator<Object[]> hashToHashJoin(
      final JoinType type,
      final JoinAlias driving,
      final JoinAlias target,
      final boolean revert
  )
  {
    while (driving.iterator.hasNext()) {
      final Map.Entry<JoinKey, Object> entry = driving.iterator.next();
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
        return product(drivingRow, otherRows, revert);
      }
      if (type == JoinType.LO) {
        return Iterators.singletonIterator(Arrays.copyOf(drivingRow, drivingRow.length + target.columns.size()));
      }
    }
    return null;
  }

  static class JoinKey implements Comparable<JoinKey>
  {
    static JoinKey hashKey(Object[] row, int[] indices)
    {
      return new JoinKey(row, indices);
    }

    final Object[] row;
    final int[] indices;

    private JoinKey(Object[] row, int[] indices)
    {
      this.row = row;
      this.indices = indices;
    }

    @Override
    public int hashCode()
    {
      int result = 1;
      for (int index : indices) {
        result = 31 * result + Objects.hashCode(row[index]);
      }
      return result;
    }

    @Override
    public boolean equals(Object other)
    {
      final JoinKey otherKey = (JoinKey) other;
      for (int i = 0; i < indices.length; i++) {
        if (!Objects.equals(row[indices[i]], otherKey.row[otherKey.indices[i]])) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int compareTo(JoinKey o)
    {
      for (int i = 0; i < indices.length; i++) {
        final int compare = Comparators.compareNF((Comparable) row[indices[i]], (Comparable) o.row[o.indices[i]]);
        if (compare != 0) {
          return compare;
        }
      }
      return 0;
    }
  }

  static final class JoinAlias implements Closeable
  {
    private static final Logger LOG = new Logger(JoinAlias.class);

    final List<String> alias;
    final List<String> columns;
    final List<String> joinColumns;
    final int estimatedNumRows;
    final Supplier<List<List<OrderByColumnSpec>>> collations;  // set after inner query is executed
    final int[] indices;
    final PeekingIterator<Object[]> rows;
    final Map<JoinKey, Object> hashed;

    Iterator<Map.Entry<JoinKey, Object>> iterator;  // hash iterator
    List<Object[]> partition;

    // sorted on key
    JoinAlias(
        List<String> alias,
        List<String> columns,
        List<String> joinColumns,
        int[] indices,
        List<Object[]> rows
    )
    {
      this.alias = alias;
      this.columns = columns;
      this.joinColumns = joinColumns;
      this.collations = Suppliers.ofInstance(Arrays.asList(OrderByColumnSpec.ascending(joinColumns)));
      this.indices = indices;
      this.rows = GuavaUtils.peekingIterator(rows.iterator());
      this.hashed = null;
      this.estimatedNumRows = rows.size();
      LOG.info("-- %s = sorted (%s:%s)(numRows=%d)", alias, joinColumns, columns, rows.size());
    }

    JoinAlias(
        List<String> alias,
        List<String> columns,
        List<String> joinColumns,
        Supplier<List<List<OrderByColumnSpec>>> collations,
        int[] indices,
        Iterator<Object[]> rows,
        int estimatedNumRows
    )
    {
      this.alias = alias;
      this.columns = columns;
      this.joinColumns = joinColumns;
      this.collations = collations == null ? NO_COLLATION : collations;
      this.indices = indices;
      this.rows = GuavaUtils.peekingIterator(rows);
      this.hashed = null;
      this.estimatedNumRows = estimatedNumRows;
      LOG.info("-- %s = stream (%s:%s)(estimated=%d)", alias, joinColumns, columns, estimatedNumRows);
    }

    JoinAlias(
        List<String> alias,
        List<String> columns,
        List<String> joinColumns,
        int[] indices,
        Iterator<Object[]> iterator
    )
    {
      this.alias = alias;
      this.columns = columns;
      this.joinColumns = joinColumns;
      this.collations = NO_COLLATION;
      this.indices = indices;
      this.rows = Iterators.peekingIterator(Collections.emptyIterator());
      this.hashed = hash(iterator, indices);
      this.estimatedNumRows = hashed.size();
      LOG.info("-- %s = hashed (%s:%s)(group=%d)", alias, joinColumns, columns, hashed.size());
    }

    private List<OrderByColumnSpec> asCollation()
    {
      for (List<OrderByColumnSpec> collation : collations.get()) {
        if (GuavaUtils.startsWith(OrderByColumnSpec.getColumns(collation), joinColumns)) {
          return collation;
        }
      }
      return OrderByColumnSpec.ascending(joinColumns);
    }

    private boolean isHashed()
    {
      return hashed != null;
    }

    private boolean isSorted()
    {
      // todo: regarded as ascending
      for (List<OrderByColumnSpec> collation : collations.get()) {
        if (GuavaUtils.startsWith(OrderByColumnSpec.getColumns(collation), joinColumns)) {
          return true;
        }
      }
      return false;
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
            Iterables.concat(Iterables.transform(hashed.values(), JoinProcessor::asValues))
        );
        hashed.clear();
      } else {
        materialized = Lists.<Object[]>newArrayList(rows);
      }
      return materialized;
    }

    private JoinAlias tryHashOnEstimation(int hashThreshold)
    {
      if (estimatedNumRows > 0 && hashThreshold > 0 && estimatedNumRows < hashThreshold) {
        return new JoinAlias(alias, columns, joinColumns, indices, rows);
      }
      return null;
    }

    // with materialization
    private JoinAlias hashOrSort(int hashThreshold)
    {
      final List<Object[]> materialized = Lists.newArrayList(rows);
      if (hashThreshold > 0 && materialized.size() < hashThreshold) {
        return new JoinAlias(alias, columns, joinColumns, indices, materialized.iterator());
      }
      return new JoinAlias(alias, columns, joinColumns, indices, sort(alias, materialized, indices));
    }

    private JoinAlias sortAll()
    {
      final List<Object[]> materialized = Lists.newArrayList(rows);
      return new JoinAlias(alias, columns, joinColumns, indices, sort(alias, materialized, indices));
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

    private List<Object[]> getHashed(JoinKey joinKey)
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
      return alias + "." + joinColumns +
             (isHashed() ? "(hashed:" : isSorted() ? "(sorted-stream:" : "(stream:") +
             (estimatedNumRows > 0 ? estimatedNumRows : "?") + ")";
    }
  }

  static class JoinResult
  {
    static JoinResult none(Iterator<Object[]> iterator)
    {
      return new JoinResult(iterator, ImmutableList.of());
    }

    static JoinResult left(JoinIterator iterator)
    {
      return new JoinResult(iterator, Arrays.asList(iterator.leftAlias.asCollation()));
    }

    static JoinResult right(JoinIterator iterator)
    {
      return new JoinResult(iterator, Arrays.asList(iterator.rightAlias.asCollation()));
    }

    static JoinResult sortMerge(JoinIterator iterator)
    {
      return new JoinResult(iterator, iterator.asCollations());
    }

    final Iterator<Object[]> iterator;
    final List<List<OrderByColumnSpec>> collations;

    private JoinResult(Iterator<Object[]> iterator, List<List<OrderByColumnSpec>> collations)
    {
      this.iterator = iterator;
      this.collations = collations;
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
      return Collections.emptyIterator();
    }
    if (left.size() == 1) {
      return product(left.get(0), right, revert);
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

  private Iterator<Object[]> product(final Object[] left, final List<Object[]> right, final boolean revert)
  {
    if (right.size() == 1) {
      return Iterators.singletonIterator(concat(left, right.get(0), revert));
    }
    return Iterators.transform(right.iterator(), new Function<Object[], Object[]>()
    {
      @Override
      public Object[] apply(Object[] right)
      {
        return concat(left, right, revert);
      }
    });
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
      final Object[] concat = Arrays.copyOf(right, left.length + right.length);
      System.arraycopy(left, 0, concat, right.length, left.length);
      return concat;
    } else {
      final Object[] concat = Arrays.copyOf(left, left.length + right.length);
      System.arraycopy(right, 0, concat, left.length, right.length);
      return concat;
    }
  }

  // from source.. need prefix for value
  private static List<Object[]> sort(List<String> alias, List<Object[]> rows, int[] indices)
  {
    final Object[][] array = rows.toArray(new Object[0][]);
    Arrays.parallelSort(array, Comparators.toArrayComparator(indices));
    return Arrays.asList(array);
  }

  static Map<JoinKey, Object> hash(final Iterator<Object[]> sequence, final int[] indices)
  {
    try {
      final Map<JoinKey, Object> hashed = Maps.newHashMap();
      while (sequence.hasNext()) {
        hashed.compute(JoinKey.hashKey(sequence.next(), indices), HASH_POPULATOR);
      }
      return hashed;
    }
    catch (Exception e) {
      if (sequence instanceof Closeable) {
        IOUtils.closeQuietly((Closeable) sequence);
      }
      throw Throwables.propagate(e);
    }
  }

  static final BiFunction<JoinKey, Object, Object> HASH_POPULATOR = new BiFunction<JoinKey, Object, Object>()
  {
    @Override
    @SuppressWarnings("unchecked")
    public Object apply(JoinKey key, Object prev)
    {
      if (prev == null) {
        return key.row;
      }
      if (prev instanceof List) {
        ((List) prev).add(key.row);
        return prev;
      }
      return Lists.newArrayList(prev, key.row);
    }
  };

  protected Supplier<List<List<OrderByColumnSpec>>> getCollations(Query<?> query)
  {
    if (query instanceof OrderingSupport) {
      List<OrderByColumnSpec> ordering = ((OrderingSupport<?>) query).getResultOrdering();
      if (ordering != null) {
        return Suppliers.ofInstance(Arrays.asList(ordering));
      }
    }
    if (query instanceof JoinQuery.JoinHolder) {
      return () -> ((JoinQuery.JoinHolder) query).getCollations();
    }
    return NO_COLLATION;
  }
}
