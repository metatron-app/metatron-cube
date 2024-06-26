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
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import io.druid.common.guava.Comparators;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.CompactRow;
import io.druid.data.input.Rows;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.segment.column.Column;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.mutable.MutableInt;

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
  protected final Logger LOG = new Logger(getClass());

  protected final JoinQueryConfig config;
  protected final int maxOutputRow;

  public JoinProcessor(JoinQueryConfig config, int maxOutputRow)
  {
    this.config = config;
    this.maxOutputRow = config == null ? maxOutputRow : config.getMaxOutputRow(maxOutputRow);
  }

  @VisibleForTesting
  final JoinResult join(JoinType type, JoinAlias left, JoinAlias right, int[] projection)
  {
    Preconditions.checkArgument(left.joinColumns.size() == right.joinColumns.size());
    if (left.joinColumns.size() == 0) {
      return product(left, right, projection);
    }
    if (left.isHashed() && right.isHashed()) {
      switch (type) {
        case INNER:
          boolean leftDriving = left.hashed.size() < right.hashed.size();
          return joinHashed(left, right, type, leftDriving, projection);
        case LO:
          return joinHashed(left, right, type, true, projection);
        case RO:
          return joinHashed(left, right, type, false, projection);
        default:
          throw new ISE("Cannot %s join with hash", type);
      }
    } else if (type.isLeftDrivable() && right.isHashed()) {
      return joinHashed(left, right, type, true, projection);
    } else if (type.isRightDrivable() && left.isHashed()) {
      return joinHashed(left, right, type, false, projection);
    }
    if (type.isLeftDrivable() && !right.isSorted() && right.estimatedNumRows > 0) {
      JoinAlias hashed = right.tryHashOnEstimation(config.getHashJoinThreshold());
      if (hashed != null) {
        return joinHashed(left, hashed, type, true, projection);
      }
    }
    if (type.isRightDrivable() && !left.isSorted() && left.estimatedNumRows > 0) {
      JoinAlias hashed = left.tryHashOnEstimation(config.getHashJoinThreshold());
      if (hashed != null) {
        return joinHashed(hashed, right, type, false, projection);
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
        return joinHashed(left, right, type, false, projection);
      }
      Preconditions.checkArgument(left.isSorted());
    }
    if (!right.isSorted()) {
      right = right.hashOrSort(type.isLeftDrivable() ? config.getHashJoinThreshold() : -1);
      if (right.isHashed()) {
        return joinHashed(left, right, type, true, projection);
      }
      Preconditions.checkArgument(right.isSorted());
    }
    return joinSorted(left, right, type, projection);
  }

  private JoinResult joinSorted(JoinAlias left, JoinAlias right, JoinType type, int[] projection)
  {
    LOG.debug(">> %s (%s --> %s) (SortedMerge)", type, left.start(), right.start());
    return JoinResult.sortMerge(new JoinIterator(type, left, right, maxOutputRow)
    {
      @Override
      protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
      {
        return mergeJoin(type, leftAlias, rightAlias, projection);
      }
    });
  }

  private JoinResult joinHashed(
      final JoinAlias left,
      final JoinAlias right,
      final JoinType type,
      final boolean leftDriving,
      final int[] projection
  )
  {
    LOG.debug(">> %s (%s %s %s) (%s + %s)", type, left.start(), leftDriving ? "->" : "<-", right.start(), left.columns, right.columns);
    if (leftDriving) {
      if (left.isHashed()) {
        return JoinResult.none(new JoinIterator(type, left.prepareHashIterator(), right, maxOutputRow)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return hashToHashJoin(type, leftAlias, rightAlias, false, projection);
          }
        });
      }
      if (left.isSorted()) {
        return JoinResult.left(new JoinIterator(type, left, right, maxOutputRow)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return sortedToHashJoin(type, leftAlias, rightAlias, false, projection);
          }
        });
      }
      return JoinResult.none(new JoinIterator(type, left, right, maxOutputRow)
      {
        @Override
        protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
        {
          return hashJoin(type, leftAlias, rightAlias, false, projection);
        }
      });
    } else {
      int[] reverted = inverse(projection, left.columns.size(), right.columns.size());
      if (right.isHashed()) {
        return JoinResult.none(new JoinIterator(type, left, right.prepareHashIterator(), maxOutputRow)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return hashToHashJoin(type, rightAlias, leftAlias, true, reverted);
          }
        });
      }
      if (right.isSorted()) {
        return JoinResult.right(new JoinIterator(type, left, right, maxOutputRow)
        {
          @Override
          protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
          {
            return sortedToHashJoin(type, rightAlias, leftAlias, true, reverted);
          }
        });
      }
      return JoinResult.none(new JoinIterator(type, left, right, maxOutputRow)
      {
        @Override
        protected Iterator<Object[]> next(JoinType type, JoinAlias leftAlias, JoinAlias rightAlias)
        {
          return hashJoin(type, rightAlias, leftAlias, true, reverted);
        }
      });
    }
  }

  private static int[] inverse(int[] projection, int left, int right)
  {
    int[] reverted = projection != null ? Arrays.copyOf(projection, projection.length) : GuavaUtils.intsFromTo(0, left + right);
    for (int i = 0; i < reverted.length; i++) {
      reverted[i] += reverted[i] < left ? right : -left;
    }
    return reverted;
  }

  private abstract class JoinIterator implements CloseableIterator<Object[]>
  {
    final JoinType type;
    final JoinAlias leftAlias;
    final JoinAlias rightAlias;
    final int limit;

    final long start = System.currentTimeMillis();

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
      LOG.debug(
          "<< %s (%s + %s), resulting %d rows (%s+%s) : (%,d msec)",
          type,
          leftAlias.end(),
          rightAlias.end(),
          count,
          leftAlias.columns,
          rightAlias.columns,
          System.currentTimeMillis() - start
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

  private Iterator<Object[]> mergeJoin(
      final JoinType type,
      final JoinAlias left,
      final JoinAlias right,
      final int[] projection
  )
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
        Iterator<Object[]> product = product(left.partition, right.partition, projection);
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
            Iterator<Object[]> lo = lo(left.partition.iterator(), right.columns.size(), projection);
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
            Iterator<Object[]> ro = ro(left.columns.size(), right.partition.iterator(), projection);
            right.partition = right.next();
            return ro;
          }
          continue;
        case FULL:
          if (compare < 0) {
            Iterator<Object[]> lo = lo(left.partition.iterator(), right.columns.size(), projection);
            left.partition = left.next();
            return lo;
          } else {
            Iterator<Object[]> ro = ro(left.columns.size(), right.partition.iterator(), projection);
            right.partition = right.next();
            return ro;
          }
        default:
          throw new UnsupportedOperationException("not supported type " + type);
      }
    }
    if (left.partition != null && (type == JoinType.LO || type == JoinType.FULL)) {
      Iterator<Object[]> lo = lo(Iterators.concat(left.partition.iterator(), left.rows), right.columns.size(), projection);
      left.partition = null;
      return lo;
    } else if (right.partition != null && (type == JoinType.RO || type == JoinType.FULL)) {
      Iterator<Object[]> ro = ro(left.columns.size(), Iterators.concat(right.partition.iterator(), right.rows), projection);
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
      final JoinAlias driver,
      final JoinAlias target,
      final boolean inverse,
      final int[] projection
  )
  {
    for (driver.partition = driver.next(); driver.partition != null; driver.partition = driver.next()) {
      final List<Object[]> targetRows = target.getHashed(JoinKey.hashKey(driver.partition.get(0), driver.indices));
      if (targetRows != null) {
        return product(driver.partition, targetRows, projection);
      }
      if (type != JoinType.INNER) {
        return inverse ? ro(target.columns.size(), driver.partition.iterator(), projection)
                      : lo(driver.partition.iterator(), target.columns.size(), projection);
      }
    }
    return null;
  }

  private Iterator<Object[]> hashToHashJoin(
      final JoinType type,
      final JoinAlias driver,
      final JoinAlias target,
      final boolean inverse,
      final int[] projection
  )
  {
    while (driver.iterator.hasNext()) {
      final Map.Entry<JoinKey, Object> entry = driver.iterator.next();
      final List<Object[]> targetRows = target.getHashed(entry.getKey());
      if (targetRows != null) {
        final List<Object[]> driveRows = asValues(entry.getValue());
        return product(driveRows, targetRows, projection);
      }
      if (type != JoinType.INNER) {
        final List<Object[]> driveRows = asValues(entry.getValue());
        return inverse ? ro(target.columns.size(), driveRows.iterator(), projection)
                      : lo(driveRows.iterator(), target.columns.size(), projection);
      }
    }
    return null;
  }

  private Iterator<Object[]> hashJoin(
      final JoinType type,
      final JoinAlias driver,
      final JoinAlias target,
      final boolean inverse,
      final int[] projection
  )
  {
    while (driver.rows.hasNext()) {
      final Object[] drivingRow = driver.rows.next();
      final List<Object[]> otherRows = target.getHashed(JoinKey.hashKey(drivingRow, driver.indices));
      if (otherRows != null) {
        return product(drivingRow, otherRows, projection);
      }
      if (type != JoinType.INNER) {
        return Iterators.singletonIterator(
            inverse ? ro(target.columns.size(), drivingRow, projection)
                    : lo(drivingRow, target.columns.size(), projection));
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
    final int hashSize;
    final MutableInt counter = new MutableInt();

    final MutableInt matched = new MutableInt();
    final MutableInt missed = new MutableInt();

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
      this.rows = peek(rows.iterator(), counter);
      this.hashed = null;
      this.hashSize = 0;
      this.estimatedNumRows = rows.size();
      LOG.debug("-- %s = sorted (%s:%s)(numRows=%d)", alias, joinColumns, columns, rows.size());
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
      this.collations = collations == null ? DataSources.NO_COLLATION : collations;
      this.indices = indices;
      this.rows = peek(rows, counter);
      this.hashed = null;
      this.hashSize = 0;
      this.estimatedNumRows = estimatedNumRows;
      LOG.debug("-- %s = stream (%s:%s)(estimated=%d)", alias, joinColumns, columns, estimatedNumRows);
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
      this.collations = DataSources.NO_COLLATION;
      this.indices = indices;
      this.rows = Iterators.peekingIterator(Collections.emptyIterator());
      this.hashed = hash(peek(iterator, counter), indices);
      this.hashSize = hashed.size();
      this.estimatedNumRows = counter.intValue();
      LOG.debug("-- %s = hashed (%s:%s)(group=%d)", alias, joinColumns, columns, hashSize);
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
        materialized = Lists.<Object[]>newArrayList(GuavaUtils.explode(hashed.values(), JoinProcessor::asValues));
      } else {
        materialized = Lists.<Object[]>newArrayList(rows);
      }
      IOUtils.closeQuietly(this);
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
      List<Object[]> match = asValues(hashed.get(joinKey));
      (match != null ? matched : missed).increment();
      return match;
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

    public String start()
    {
      return describe(true);
    }

    public String end()
    {
      return describe(false);
    }

    private String describe(boolean start)
    {
      if (isHashed()) {
        if (start) {
          return String.format("%s.%s(hashed:%d/%d)", alias, joinColumns, hashSize, counter.intValue());
        }
        return String.format(
            "%s.%s(hashed:%d/%d)(matched=%d, missed=%d)",
            alias, joinColumns, hashSize, counter.intValue(), matched.intValue(), missed.intValue()
        );
      }
      String prefix = isSorted() ? "sorted-stream" : "stream";
      String row = counter.intValue() > 0 ? String.valueOf(counter) : estimatedNumRows > 0 ? estimatedNumRows + "?" : "?";
      return String.format("%s.%s(%s:%s)", alias, joinColumns, prefix, row);
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
      return ImmutableList.<Object[]>of((Object[]) value);
    }
  }

  private JoinResult product(final JoinAlias left, final JoinAlias right, final int[] projection)
  {
    LOG.debug(">> CROSS (%s x %s)", left.start(), right.start());
    List<Object[]> leftRows = left.materialize();
    List<Object[]> rightRows = right.materialize();
    return JoinResult.none(GuavaUtils.withResource(product(leftRows, rightRows, projection), () ->
        LOG.debug("<< CROSS (%s + %s), resulting %d rows (%s+%s)",
                  left.end(), right.end(), leftRows.size() * rightRows.size(), left.columns, right.columns
        )));
  }

  private static Iterator<Object[]> product(List<Object[]> driver, List<Object[]> target, int[] projection)
  {
    if (driver.isEmpty() || target.isEmpty()) {
      return Collections.emptyIterator();
    }
    if (driver.size() == 1) {
      return product(driver.get(0), target, projection);
    }
    if (target.size() == 1) {
      return product(driver, target.get(0), projection);
    }
    return new Iterator<Object[]>()
    {
      private int l;
      private int r;

      @Override
      public boolean hasNext()
      {
        return r < target.size() || l + 1 < driver.size();
      }

      @Override
      public Object[] next()
      {
        if (r < target.size()) {
          return concat(driver.get(l), target.get(r++), projection);
        }
        if (l + 1 < driver.size()) {
          r = 0;
          return concat(driver.get(++l), target.get(r++), projection);
        }
        throw new NoSuchElementException();
      }
    };
  }

  private static Iterator<Object[]> product(List<Object[]> left, Object[] right, int[] projection)
  {
    if (left.size() == 1) {
      return Iterators.singletonIterator(concat(left.get(0), right, projection));
    }
    return Iterators.transform(left.iterator(), row -> concat(row, right, projection));
  }

  private static Iterator<Object[]> product(Object[] left, List<Object[]> right, int[] projection)
  {
    if (right.size() == 1) {
      return Iterators.singletonIterator(concat(left, right.get(0), projection));
    }
    return Iterators.transform(right.iterator(), row -> concat(left, row, projection));
  }

  private static Object[] lo(Object[] row, int right, int[] projection)
  {
    if (projection == null) {
      return Arrays.copyOf(row, row.length + right);
    }
    final Object[] join = new Object[projection.length];
    for (int i = 0; i < join.length; i++) {
      join[i] = projection[i] < row.length ? row[projection[i]] : null;
    }
    return join;
  }

  private static Iterator<Object[]> lo(Iterator<Object[]> rows, int right, int[] projection)
  {
    return Iterators.transform(rows, row -> lo(row, right, projection));
  }

  private static Object[] ro(int left, Object[] row, int[] projection)
  {
    if (projection == null) {
      Object[] concat = new Object[left + row.length];
      System.arraycopy(row, 0, concat, left, row.length);
      return concat;
    }
    final Object[] join = new Object[projection.length];
    for (int i = 0; i < join.length; i++) {
      join[i] = projection[i] < row.length ? row[projection[i]] : null;
    }
    return join;
  }

  private static Iterator<Object[]> ro(int rows, Iterator<Object[]> right, int[] projection)
  {
    return Iterators.transform(right, row -> ro(rows, row, projection));
  }

  private static Object[] concat(Object[] left, Object[] right, int[] projection)
  {
    if (projection == null) {
      Object[] concat = Arrays.copyOf(left, left.length + right.length);
      System.arraycopy(right, 0, concat, left.length, right.length);
      return concat;
    }
    Object[] concat = new Object[projection.length];
    for (int i = 0; i < concat.length; i++) {
      concat[i] = projection[i] < left.length ? left[projection[i]] : right[projection[i] - left.length];
    }
    return concat;
  }

  // from source.. need prefix for value
  private static List<Object[]> sort(List<String> alias, List<Object[]> rows, int[] indices)
  {
    Object[][] array = rows.toArray(new Object[0][]);
    Arrays.parallelSort(array, Comparators.toArrayComparator(indices));
    return Arrays.asList(array);
  }

  private static Map<JoinKey, Object> hash(Iterator<Object[]> sequence, int[] indices)
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
      throw QueryException.wrapIfNeeded(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static final BiFunction<JoinKey, Object, Object> HASH_POPULATOR = (key, prev) ->
  {
    if (prev == null) {
      return key.row;
    }
    if (prev instanceof List) {
      ((List) prev).add(key.row);
      return prev;
    }
    return Lists.newArrayList(prev, key.row);
  };

  private static <T> PeekingIterator<T> peek(Iterator<? extends T> iterator, MutableInt counter)
  {
    return new GuavaUtils.DelegatedPeekingIterator<T>(Iterators.peekingIterator(iterator))
    {
      @Override
      public T next()
      {
        counter.increment();
        return delegated.next();
      }
    };
  }

  @SuppressWarnings("unchecked")
  protected static Sequence format(Iterator projected, List<String> projectedNames, boolean asMap, boolean asRow)
  {
    if (asMap) {
      Iterator iterator = GuavaUtils.map(projected, Rows.arrayToMap(projectedNames));
      return Sequences.once(
          projectedNames, asRow ? GuavaUtils.map(iterator, Rows.mapToRow(Column.TIME_COLUMN_NAME)) : iterator
      );
    }
    return Sequences.once(projectedNames, asRow ? GuavaUtils.map(projected, CompactRow.WRAP) : projected);
  }
}
