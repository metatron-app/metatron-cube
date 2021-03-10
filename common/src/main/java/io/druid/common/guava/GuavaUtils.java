/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.common.guava;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import io.druid.common.IntTagged;
import io.druid.common.Progressing;
import io.druid.concurrent.PrioritizedCallable;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.CloseableIterator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.mutable.MutableInt;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 */
public class GuavaUtils
{
  private static final Logger LOG = new Logger(GuavaUtils.class);

  // null check in Ordering.natural() prevents unrolling in some cases
  @SuppressWarnings("unchecked")
  public static final Comparator NO_NULLABLE_NATURAL =
      new Comparator()
      {
        @Override
        public int compare(final Object o1, final Object o2)
        {
          return o1 == o2 ? 0 : ((Comparable) o1).compareTo(o2);
        }
      };

  @SuppressWarnings("unchecked")
  public static final Comparator NULL_FIRST_NATURAL = Comparators.NULL_FIRST(NO_NULLABLE_NATURAL);

  public static final Comparator TIME_COMPARATOR =
      (t1, t2) -> Long.compare(((Number) t1).longValue(), ((Number) t2).longValue());

  @SuppressWarnings("unchecked")
  public static <T> Comparator<T> noNullableNatural()
  {
    return NO_NULLABLE_NATURAL;
  }

  @SuppressWarnings("unchecked")
  public static <T> Comparator<T> nullFirstNatural()
  {
    return NULL_FIRST_NATURAL;
  }

  public static <T> Comparator<T> allEquals()
  {
    return (left, right) -> 0;
  }

  public static Function<Object, String> NULLABLE_TO_STRING_FUNC = new Function<Object, String>()
  {
    @Override
    public String apply(Object input) { return Objects.toString(input, null);}
  };

  public static Function<String, String> formatFunction(final String formatString)
  {
    return new Function<String, String>()
    {
      @Override
      public String apply(@Nullable String input)
      {
        return String.format(formatString, input);
      }
    };
  }

  @Nullable
  public static Long tryParseLong(@Nullable String string)
  {
    return Strings.isNullOrEmpty(string)
           ? null
           : Longs.tryParse(string.charAt(0) == '+' ? string.substring(1) : string);
  }

  @SuppressWarnings("unchecked")
  public static <X, Y> List<Y> cast(Collection<X> input)
  {
    if (input == null) {
      return null;
    }
    List<Y> casted = Lists.<Y>newArrayListWithCapacity(input.size());
    for (X x : input) {
      casted.add((Y) x);
    }
    return casted;
  }

  public static <X, Y> Function<X, Y> caster()
  {
    return new Function<X, Y>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Y apply(X input)
      {
        return (Y)input;
      }
    };
  }

  public static <A, B> List<Pair<A, B>> zip(List<A> as, List<B> bs)
  {
    Preconditions.checkArgument(as.size() == bs.size(), "size differs.. %s vs %s", as.size(), bs.size());
    List<Pair<A, B>> result = Lists.newArrayListWithCapacity(as.size());
    for (int i = 0; i < as.size(); i++) {
      result.add(Pair.of(as.get(i), bs.get(i)));
    }
    return result;
  }

  public static <A, B> Pair<List<A>, List<B>> unzip(Map<A, B> map)
  {
    return Pair.of(ImmutableList.copyOf(map.keySet()), ImmutableList.copyOf(map.values()));
  }

  public static <T> List<IntTagged<T>> zipWithIndex(Iterable<T> as)
  {
    return Lists.newArrayList(
        Iterables.transform(
            as, new Function<T, IntTagged<T>>()
            {
              private int indexer;

              @Override
              public IntTagged<T> apply(T input)
              {
                return IntTagged.of(indexer++, input);
              }
            }
        )
    );
  }

  public static <A, B> Map<A, B> asMap(Iterable<Pair<A, B>> pairs)
  {
    Map<A, B> map = Maps.newLinkedHashMap();
    for (Pair<A, B> pair : pairs) {
      map.put(pair.lhs, pair.rhs);
    }
    return map;
  }

  public static <A, B> Map<A, B> zipAsMap(List<A> as, List<B> bs)
  {
    Preconditions.checkArgument(as.size() == bs.size(), "size differs.. %d vs %s", as.size(), bs.size());
    Map<A, B> map = Maps.newLinkedHashMap();
    for (int i = 0; i < as.size(); i++) {
      map.put(as.get(i), bs.get(i));
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  public static <A, B> Map<A, B> asMap(Object... keyValues)
  {
    Map<A, B> map = Maps.newLinkedHashMap();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put((A) keyValues[i], (B) keyValues[i + 1]);
    }
    return map;
  }

  // ~Functions.compose()
  public static <F, X, T> Function<F, T> sequence(final Function<F, X> f, final Function<X, T> t)
  {
    return new Function<F, T>()
    {
      @Override
      public T apply(final F input)
      {
        return t.apply(f.apply(input));
      }
    };
  }

  public static <F, X, Y, T> Function<F, T> sequence(
      final Function<F, X> f,
      final Function<X, Y> m,
      final Function<Y, T> t
  )
  {
    return new Function<F, T>()
    {
      @Override
      public T apply(final F input)
      {
        return t.apply(m.apply(f.apply(input)));
      }
    };
  }

  public static <F, X, Y, Z, T> Function<F, T> sequence(
      final Function<F, X> f,
      final Function<X, Y> m1,
      final Function<Y, Z> m2,
      final Function<Z, T> t
  )
  {
    return new Function<F, T>()
    {
      @Override
      public T apply(final F input)
      {
        return t.apply(m2.apply(m1.apply(f.apply(input))));
      }
    };
  }

  public static List<String> exclude(Iterable<String> name, String... exclusions)
  {
    return exclude(name, Arrays.asList(exclusions));
  }

  public static List<String> exclude(Iterable<String> name, Collection<String> exclusions)
  {
    if (name == null) {
      return Lists.<String>newArrayList();
    }
    List<String> retaining = Lists.newArrayList(name);
    if (exclusions != null) {
      retaining.removeAll(exclusions);
    }
    return retaining;
  }

  public static List<String> prependEach(String prepend, List<String> list)
  {
    List<String> prepended = Lists.newArrayList();
    for (String element : list) {
      prepended.add(prepend + element);
    }
    return prepended;
  }

  @SafeVarargs
  public static <T> List<T> concat(Iterable<T> iterable, T... elements)
  {
    if (iterable == null) {
      return Arrays.asList(elements);
    }
    List<T> concat = Lists.newArrayList(iterable);
    concat.addAll(Arrays.asList(elements));
    return concat;
  }

  @SafeVarargs
  public static <T> T[] concatArray(Iterable<T> iterable, T... elements)
  {
    return iterable == null ? elements : concat(iterable, elements).toArray(elements);
  }

  public static <T> List<T> concat(T element, List<T> list2)
  {
    return concat(Arrays.asList(element), list2);
  }

  public static <T> List<T> concat(T element, Iterable<T> list2)
  {
    return concat(element, Lists.newArrayList(list2));
  }

  public static <T> List<T> concat(List<T> list1, List<T> list2)
  {
    if (list1 == null && list2 == null) {
      return Lists.newArrayList();
    }
    if (list1 == null) {
      return list2;
    }
    if (list2 == null) {
      return list1;
    }
    return Lists.newArrayList(Iterables.concat(list1, list2));
  }

  public static <T> List<T> concat(List<T> element, Iterable<T> list2)
  {
    return Lists.newArrayList(Iterables.concat(element, list2));
  }

  @SafeVarargs
  public static <T> List<T> concat(List<T>... lists)
  {
    return Lists.newArrayList(Iterables.concat(lists));
  }

  @SafeVarargs
  public static <T> List<T> dedupConcat(List<T>... lists)
  {
    Iterator<List<T>> iterator = Iterables.filter(Arrays.asList(lists), Predicates.notNull()).iterator();
    if (!iterator.hasNext()) {
      return Lists.newArrayList();
    }
    Set<T> set = Sets.newLinkedHashSet(iterator.next());
    while (iterator.hasNext()) {
      for (T element : iterator.next()) {
        if (!set.contains(element)) {
          set.add(element);
        }
      }
    }
    return Lists.newArrayList(set);
  }

  @SuppressWarnings("unchecked")
  public static <T, X extends T> X firstOf(List<T> list)
  {
    return list.isEmpty() ? null : (X) list.get(0);
  }

  @SuppressWarnings("unchecked")
  public static <T, X extends T> X lastOf(List<T> list)
  {
    return list.isEmpty() ? null : (X) list.get(list.size() - 1);
  }

  public static <T> T get(List<T> list, int x)
  {
    return list == null || x >= list.size() ? null : list.get(x);
  }

  public static int[] indexOf(List<String> list, List<String> indexing)
  {
    return indexOf(list, indexing, false);
  }

  public static int[] indexOf(List<String> list, List<String> indexing, boolean assertExistence)
  {
    final int[] indices = new int[indexing.size()];
    for (int i = 0; i < indices.length; i++) {
      final int x = list.indexOf(indexing.get(i));
      if (assertExistence && x < 0) {
        return null;
      }
      indices[i] = x;
    }
    return indices;
  }

  public static <T> Pair<List<T>, List<T>> partition(Iterable<T> iterable, Predicate<T> predicate)
  {
    final List<T> trues = Lists.newArrayList();
    final List<T> falses = Lists.newArrayList();
    for (T e : iterable) {
      if (predicate.apply(e)) {
        trues.add(e);
      } else {
        falses.add(e);
      }
    }
    return Pair.of(trues, falses);
  }

  public static int[] checkedCast(long[] longs)
  {
    int[] ints = new int[longs.length];
    for (int i = 0; i < ints.length; i++) {
      if (longs[i] != (int) longs[i]) {
        return null;
      }
      ints[i] = (int) longs[i];
    }
    return ints;
  }

  public static int[] intsTo(int end)
  {
    return intsFromTo(0, end);
  }

  public static int[] intsFromTo(int start, int end)
  {
    final int[] ints = new int[end - start];
    for (int i = 0; i < ints.length; i++) {
      ints[i] = start + i;
    }
    return ints;
  }

  public static boolean isIdenticalIndex(int[] indices)
  {
    for (int i = 0; i < indices.length; i++) {
      if (i != indices[i]) {
        return false;
      }
    }
    return true;
  }

  public static double[] castDouble(long[] longs)
  {
    double[] doubles = new double[longs.length];
    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = (double) longs[i];
    }
    return doubles;
  }

  public static <K, V> void add(Map<K, List<V>> map, K key, V value)
  {
    List<V> list = map.get(key);
    if (list == null) {
      map.put(key, list = Lists.newArrayList());
    }
    list.add(value);
  }

  public static boolean isNullOrEmpty(Collection<?> collection)
  {
    return collection == null || collection.isEmpty();
  }

  public static boolean isNullOrEmpty(Map<?, ?> collection)
  {
    return collection == null || collection.isEmpty();
  }

  public static String arrayOfArrayToString(Object[][] array)
  {
    StringBuilder b = new StringBuilder();
    for (Object[] x : array) {
      if (b.length() > 0) {
        b.append(", ");
      }
      b.append(Arrays.toString(x));
    }
    return b.toString();
  }

  public static String arrayToString(Object[] array)
  {
    StringBuilder b = new StringBuilder().append('[');
    for (Object x : array) {
      if (b.length() > 1) {
        b.append(", ");
      }
      if (x instanceof Object[]) {
        b.append(arrayToString((Object[]) x));
      } else {
        b.append(x);
      }
    }
    return b.append(']').toString();
  }

  public static boolean containsNull(List list)
  {
    return Lists.newArrayList(list).contains(null);
  }

  public static boolean containsAny(Collection collection, Collection finding)
  {
    for (Object x : finding) {
      if (collection.contains(x)) {
        return true;
      }
    }
    return false;
  }

  public static boolean containsAll(Collection collection, Collection finding)
  {
    for (Object x : finding) {
      if (!collection.contains(x)) {
        return false;
      }
    }
    return true;
  }

  public static List<String> retain(List<String> list, List<String> retain)
  {
    if (isNullOrEmpty(retain)) {
      return list;
    }
    List<String> retaining = Lists.newArrayList(list);
    retaining.retainAll(retain);
    return retaining;
  }

  public static <V> Map<String, V> retain(Map<String, V> map, List<String> retain)
  {
    if (isNullOrEmpty(retain)) {
      return map;
    }
    Map<String, V> retaining = Maps.newLinkedHashMap();
    for (String key : retain) {
      if (map.containsKey(key)) {
        retaining.put(key, map.get(key));
      }
    }
    return retaining;
  }

  public static void closeQuietly(List<? extends Closeable> resources)
  {
    for (Closeable resource : resources) {
      IOUtils.closeQuietly(resource);
    }
  }

  public static <F, T> List<T> transform(Iterable<F> fromList, Function<? super F, ? extends T> function)
  {
    return Lists.newArrayList(Iterables.transform(fromList, function));
  }

  public static <F, T> List<T> transform(List<F> fromList, Function<? super F, ? extends T> function)
  {
    return Lists.newArrayList(Iterables.transform(fromList, function));
  }

  public static Function<Object[], Object[]> mapper(final int[] indices)
  {
    if (isIdenticalIndex(indices)) {
      return Functions.identity();
    }
    return new Function<Object[], Object[]>()
    {
      @Override
      public Object[] apply(final Object[] input)
      {
        final Object[] output = new Object[indices.length];
        for (int i = 0; i < indices.length; i++) {
          output[i] = input[indices[i]];
        }
        return output;
      }
    };
  }

  public static boolean startsWith(List<String> target, List<String> starts)
  {
    return starts.isEmpty() || target.size() >= starts.size() && target.subList(0, starts.size()).equals(starts);
  }

  public static interface CloseablePeekingIterator<T> extends PeekingIterator<T>, Closeable {
  }

  public static <T> PeekingIterator<T> peekingIterator(final Iterator<? extends T> iterator)
  {
    final PeekingIterator<T> peekingIterator = Iterators.peekingIterator(iterator);
    if (iterator instanceof Closeable) {
      return new CloseablePeekingIterator<T>()
      {
        @Override
        public void close() throws IOException
        {
          ((Closeable) iterator).close();
        }

        @Override
        public T peek()
        {
          return peekingIterator.peek();
        }

        @Override
        public T next()
        {
          return peekingIterator.next();
        }

        @Override
        public void remove()
        {
          peekingIterator.remove();
        }

        @Override
        public boolean hasNext()
        {
          return peekingIterator.hasNext();
        }
      };
    }
    return peekingIterator;
  }

  public static <T> PeekingIterator<T> peekingIterator(final Iterator<? extends T> iterator, final MutableInt counter)
  {
    final PeekingIterator<T> peekingIterator = Iterators.peekingIterator(iterator);
    if (iterator instanceof Closeable) {
      return new CloseablePeekingIterator<T>()
      {
        @Override
        public void close() throws IOException
        {
          ((Closeable) iterator).close();
        }

        @Override
        public T peek()
        {
          return peekingIterator.peek();
        }

        @Override
        public T next()
        {
          counter.increment();
          return peekingIterator.next();
        }

        @Override
        public void remove()
        {
          peekingIterator.remove();
        }

        @Override
        public boolean hasNext()
        {
          return peekingIterator.hasNext();
        }
      };
    }
    return peekingIterator;
  }

  public static <F, T> Iterator<T> map(final Iterator<F> iterator, final Function<F, T> function)
  {
    Iterator<T> mapped = Iterators.transform(iterator, function);
    if (iterator instanceof Closeable) {
      return withResource(mapped, (Closeable) iterator);
    }
    return mapped;
  }

  public static <T> Iterator<T> withResource(final Iterator<T> iterator, final Closeable closeable)
  {
    return new CloseableIterator<T>()
    {
      private boolean closed;

      @Override
      public void close() throws IOException
      {
        if (!closed) {
          closed = true;
          closeable.close();
        }
      }

      @Override
      public boolean hasNext()
      {
        final boolean hasNext = !closed && iterator.hasNext();
        if (!hasNext && !closed) {
          closed = true;
          IOUtils.closeQuietly(closeable);
        }
        return hasNext;
      }

      @Override
      public T next()
      {
        try {
          return iterator.next();
        }
        catch (NoSuchElementException e) {
          if (!closed) {
            closed = true;
            IOUtils.closeQuietly(closeable);
          }
          throw e;
        }
      }

      @Override
      public void remove()
      {

      }
    };
  }

  public static class DelegatedProgressing<T> implements Progressing.OnIterator<T>
  {
    private final Iterator<T> delegate;

    public DelegatedProgressing(Iterator<T> delegate) {this.delegate = delegate;}

    @Override
    public boolean hasNext()
    {
      return delegate.hasNext();
    }

    @Override
    public T next()
    {
      return delegate.next();
    }

    @Override
    public void remove()
    {
      delegate.remove();
    }

    @Override
    public float progress()
    {
      return delegate instanceof Progressing ? ((Progressing) delegate).progress() : hasNext() ? 0 : 1;
    }

    @Override
    public void close() throws IOException
    {
      if (delegate instanceof Closeable) {
        ((Closeable) delegate).close();
      }
    }
  }


  public static <K, V> Map<K, V> mutableMap(K k1, V v1)
  {
    Map<K, V> map = Maps.newHashMap();
    map.put(k1, v1);
    return map;
  }

  public static <K, V> Map<K, V> mutableMap(K k1, V v1, K k2, V v2)
  {
    Map<K, V> map = Maps.newHashMap();
    map.put(k1, v1);
    map.put(k2, v2);
    return map;
  }

  public static <K, V> Map<K, V> mutableMap(K k1, V v1, K k2, V v2, K k3, V v3)
  {
    Map<K, V> map = Maps.newHashMap();
    map.put(k1, v1);
    map.put(k2, v2);
    map.put(k3, v3);
    return map;
  }

  public static <K, V> Map<K, V> mutableMap(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4)
  {
    Map<K, V> map = Maps.newHashMap();
    map.put(k1, v1);
    map.put(k2, v2);
    map.put(k3, v3);
    map.put(k4, v4);
    return map;
  }

  public static <K, V> Map<K, V> mutableMap(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5)
  {
    Map<K, V> map = Maps.newHashMap();
    map.put(k1, v1);
    map.put(k2, v2);
    map.put(k3, v3);
    map.put(k4, v4);
    map.put(k5, v5);
    return map;
  }

  // null for EOF version for simplification.. cannot use on null containing iterator
  public static <T> T peek(PeekingIterator<T> peeker)
  {
    return peeker.hasNext() ? peeker.peek() : null;
  }

  public static File createTemporaryDirectory(String prefix, String suffix) throws IOException
  {
    File output = File.createTempFile(prefix, suffix);
    output.delete();
    output.mkdirs();
    return output;
  }

  public static <F, T> Callable<T> asCallable(final Function<F, T> function, final F param)
  {
    return new PrioritizedCallable.Background<T>()
    {
      @Override
      public T call() throws Exception
      {
        return function.apply(param);
      }
    };
  }

  public static <T> Function<T, T> identity(final String log)
  {
    return new Function<T, T>()
    {
      @Override
      public T apply(T o)
      {
        return o;
      }

      @Override
      public String toString()
      {
        return String.format("identity(%s)", log);
      }
    };
  }

  public static void dump(Logger LOG)
  {
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    for (long threadId : threadMXBean.getAllThreadIds()) {
      LOG.info(GuavaUtils.dump(threadMXBean.getThreadInfo(threadId, Integer.MAX_VALUE)));
    }
  }

  // copied from ThreadInfo.toString() to dump all stack traces
  public static String dump(ThreadInfo thread)
  {
    StringBuilder sb = new StringBuilder(1024);

    sb.append('\"').append(thread.getThreadName()).append('\"')
      .append(" Id=").append(thread.getThreadId()).append(' ').append(thread.getThreadState());

    if (thread.getLockName() != null) {
      sb.append(" on ").append(thread.getLockName());
    }
    if (thread.getLockOwnerName() != null) {
      sb.append(" owned by \"").append(thread.getLockOwnerName()).append("\" Id=").append(thread.getLockOwnerId());
    }
    if (thread.isSuspended()) {
      sb.append(" (suspended)");
    }
    if (thread.isInNative()) {
      sb.append(" (in native)");
    }
    sb.append('\n');

    StackTraceElement[] stackTrace = thread.getStackTrace();
    for (int i = 0; i < stackTrace.length; i++) {
      StackTraceElement ste = stackTrace[i];
      sb.append("\tat ").append(ste.toString());
      sb.append('\n');
      if (i == 0 && thread.getLockInfo() != null) {
        Thread.State ts = thread.getThreadState();
        switch (ts) {
          case BLOCKED:
            sb.append("\t-  blocked on ").append(thread.getLockInfo());
            sb.append('\n');
            break;
          case WAITING:
            sb.append("\t-  waiting on ").append(thread.getLockInfo());
            sb.append('\n');
            break;
          case TIMED_WAITING:
            sb.append("\t-  waiting on ").append(thread.getLockInfo());
            sb.append('\n');
            break;
          default:
        }
      }

      for (MonitorInfo mi : thread.getLockedMonitors()) {
        if (mi.getLockedStackDepth() == i) {
          sb.append("\t-  locked ").append(mi);
          sb.append('\n');
        }
      }
    }

    LockInfo[] locks = thread.getLockedSynchronizers();
    if (locks.length > 0) {
      sb.append("\n\tNumber of locked synchronizers = ").append(locks.length);
      sb.append('\n');
      for (LockInfo li : locks) {
        sb.append("\t- ").append(li);
        sb.append('\n');
      }
    }
    sb.append('\n');
    return sb.toString();
  }
}
