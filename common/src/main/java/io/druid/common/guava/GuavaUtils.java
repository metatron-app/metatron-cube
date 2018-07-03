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

package io.druid.common.guava;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.Pair;
import io.druid.common.Progressing;
import org.apache.commons.io.IOUtils;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 */
public class GuavaUtils
{
  private static final Comparator NULL_FIRST_NATURAL = Ordering.natural().nullsFirst();

  @SuppressWarnings("unchecked")
  public static <T> Comparator<T> nullFirstNatural()
  {
    return NULL_FIRST_NATURAL;
  }

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

  public static InputSupplier<BufferedReader> joinFiles(final File... files)
  {
    return joinFiles(Arrays.asList(files));
  }

  public static InputSupplier<BufferedReader> joinFiles(final List<File> files)
  {

    return new InputSupplier<BufferedReader>()
    {
      @Override
      public BufferedReader getInput() throws IOException
      {
        return new BufferedReader(
            CharStreams.join(
                Iterables.transform(
                    files,
                    new Function<File, InputSupplier<InputStreamReader>>()
                    {
                      @Override
                      public InputSupplier<InputStreamReader> apply(final File input)
                      {
                        return new InputSupplier<InputStreamReader>()
                        {
                          @Override
                          public InputStreamReader getInput() throws IOException
                          {
                            InputStream baseStream = new FileInputStream(input);
                            if (input.getName().endsWith(".gz")) {
                              baseStream = new GZIPInputStream(baseStream);
                            }

                            return new InputStreamReader(baseStream, Charsets.UTF_8);
                          }
                        };
                      }
                    }
                )
            ).getInput()
        );
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
    Preconditions.checkArgument(as.size() == bs.size(), "size differs.. " + as.size() + " vs " + bs.size());
    List<Pair<A, B>> result = Lists.newArrayListWithCapacity(as.size());
    for (int i = 0; i < as.size(); i++) {
      result.add(Pair.of(as.get(i), bs.get(i)));
    }
    return result;
  }

  public static <T> List<Pair<T, Integer>> zipWithIndex(Iterable<T> as)
  {
    return Lists.newArrayList(
        Iterables.transform(
            as, new Function<T, Pair<T, Integer>>()
            {
              private int indexer;

              @Override
              public Pair<T, Integer> apply(T input)
              {
                return Pair.of(input, indexer++);
              }
            }
        )
    );
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
  public static <T> List<T> concat(List<T> list1, T... elements)
  {
    return concat(list1, Arrays.asList(elements));
  }

  public static <T> List<T> concat(T element, List<T> list2)
  {
    return concat(Arrays.asList(element), list2);
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

  public static int[] indexOf(List<String> list, List<String> indexing)
  {
    List<Integer> indices = Lists.newArrayList();
    for (String sort : indexing) {
      indices.add(list.indexOf(sort));
    }
    return Ints.toArray(indices);
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

  @SafeVarargs
  public static <T> List<T> dedupConcat(Iterable<T>... iterables)
  {
    Set<T> columns = Sets.newLinkedHashSet();
    for (Iterable<T> iterable : iterables) {
      for (T value : iterable) {
        if (!columns.contains(value)) {
          columns.add(value);
        }
      }
    }
    return Lists.newArrayList(columns);
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

  public static boolean containsNull(List list)
  {
    return Lists.newArrayList(list).contains(null);
  }

  public static List<String> retain(List<String> list, List<String> retain)
  {
    if (isNullOrEmpty(retain)) {
      return list;
    }
    List<String> retaining = Lists.newArrayList(list);
    retaining.retainAll(retain);
    return retain;
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

  public static interface CloseableIterator<T> extends Iterator<T>, Closeable {
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

  public static <T> Iterator<T> withResource(final Iterator<T> iterator)
  {
    if (iterator instanceof Closeable) {
      return withResource(iterator, (Closeable) iterator);
    }
    return iterator;
  }

  public static <T> Iterator<T> withResource(final Iterator<T> iterator, final Closeable closeable)
  {
    return new CloseableIterator<T>()
    {
      @Override
      public void close() throws IOException
      {
        closeable.close();
      }

      @Override
      public boolean hasNext()
      {
        final boolean hasNext = iterator.hasNext();
        if (!hasNext) {
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
          IOUtils.closeQuietly(closeable);
          throw e;
        }
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
  }
}
