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

import com.google.common.base.Preconditions;
import io.druid.data.Pair;
import it.unimi.dsi.fastutil.objects.AbstractObject2ObjectMap;
import it.unimi.dsi.fastutil.objects.AbstractObjectCollection;
import it.unimi.dsi.fastutil.objects.AbstractObjectSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVArraySortedMap<K, V> extends AbstractObject2ObjectMap<K, V> {

  private static final long serialVersionUID = 1L;

  public static <K, V> KVArraySortedMap<K, V> of(K key, V value)
  {
    return new KVArraySortedMap<K, V>(new Object[]{key}, new Object[]{value});
  }

  public static <K, V> KVArraySortedMap<K, V> incremental(K key, V value, int increment)
  {
    Preconditions.checkArgument(increment > 0);
    return new KVArraySortedMap<K, V>(new Object[]{key}, new Object[]{value})
    {
      @Override
      protected int newSize(int current)
      {
        return current + increment;
      }
    };
  }

  /** The keys (valid up to {@link #size}, excluded). */
  private transient Object[] key;
  /** The values (parallel to {@link #key}). */
  private transient Object[] value;
  /** The number of valid entries in {@link #key} and {@link #value}. */
  private int size;

  /**
   * Creates a new empty array map with given key and value backing arrays.
   * The resulting map will have as many entries as the given arrays.
   *
   * <p>
   * It is responsibility of the caller that the elements of {@code key} are
   * distinct.
   *
   * @param key
   *            the key array.
   * @param value
   *            the value array (it <em>must</em> have the same length as
   *            {@code key}).
   */
  public KVArraySortedMap(final Object[] key, final Object[] value) {
    this.key = key;
    this.value = value;
    size = key.length;
    if (key.length != value.length)
      throw new IllegalArgumentException(
          "Keys and values have different lengths (" + key.length + ", " + value.length + ")");
  }

  /**
   * Creates a new array map with given key and value backing arrays, using
   * the given number of elements.
   *
   * <p>
   * It is responsibility of the caller that the first {@code size} elements
   * of {@code key} are distinct.
   *
   * @param key
   *            the key array.
   * @param value
   *            the value array (it <em>must</em> have the same length as
   *            {@code key}).
   * @param size
   *            the number of valid elements in {@code key} and {@code value}.
   */
  public KVArraySortedMap(final Object[] key, final Object[] value, final int size) {
    this.key = key;
    this.value = value;
    this.size = size;
    if (key.length != value.length)
      throw new IllegalArgumentException(
          "Keys and values have different lengths (" + key.length + ", " + value.length + ")");
    if (size > key.length)
      throw new IllegalArgumentException("The provided size (" + size
                                         + ") is larger than or equal to the backing-arrays size (" + key.length + ")");
  }

  @SuppressWarnings("unchecked")
  public Map.Entry<K, V> lastEntry()
  {
    return Pair.of((K) key[size - 1], (V) value[size - 1]);
  }

  public Iterable<V> descendingValues()
  {
    return () -> {
      return new Iterator<V>()
      {
        private int index = size - 1;

        @Override
        public boolean hasNext()
        {
          return index >= 0;
        }

        @Override
        @SuppressWarnings("unchecked")
        public V next()
        {
          return (V) value[index--];
        }
      };
    };
  }

  public static class BasicEntry<K, V> implements Object2ObjectMap.Entry<K, V>
  {
    private K key;
    private V value;

    @Override
    public K getKey()
    {
      return key;
    }

    @Override
    public V getValue()
    {
      return value;
    }

    @Override
    public V setValue(V value)
    {
      V prev = this.value;
      this.value = value;
      return prev;
    }
  }

  private final class EntrySet extends AbstractObjectSet<Entry<K, V>> implements FastEntrySet<K, V> {

    @Override
    public ObjectIterator<Entry<K, V>> iterator() {
      return new ObjectIterator<Object2ObjectMap.Entry<K, V>>() {
        int curr = -1, next = 0;

        @Override
        public boolean hasNext() {
          return next < size;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Entry<K, V> next() {
          if (!hasNext())
            throw new NoSuchElementException();
          return new AbstractObject2ObjectMap.BasicEntry<>((K) key[curr = next], (V) value[next++]);
        }

        @Override
        public void remove() {
          if (curr == -1)
            throw new IllegalStateException();
          curr = -1;
          final int tail = size-- - next--;
          System.arraycopy(key, next + 1, key, next, tail);
          System.arraycopy(value, next + 1, value, next, tail);

          key[size] = null;

          value[size] = null;

        }
      };
    }

    @Override
    public ObjectIterator<Entry<K, V>> fastIterator() {
      return new ObjectIterator<Entry<K, V>>() {
        int next = 0, curr = -1;
        final BasicEntry<K, V> entry = new BasicEntry<>();

        @Override
        public boolean hasNext() {
          return next < size;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Entry<K, V> next() {
          if (!hasNext())
            throw new NoSuchElementException();
          entry.key = (K) key[curr = next];
          entry.value = (V) value[next++];
          return entry;
        }

        @Override
        public void remove() {
          if (curr == -1)
            throw new IllegalStateException();
          curr = -1;
          final int tail = size-- - next--;
          System.arraycopy(key, next + 1, key, next, tail);
          System.arraycopy(value, next + 1, value, next, tail);

          key[size] = null;

          value[size] = null;

        }
      };
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(Object o) {
      if (!(o instanceof Map.Entry))
        return false;
      final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;

      final K k = ((K) e.getKey());
      return KVArraySortedMap.this.containsKey(k)
             && java.util.Objects.equals(KVArraySortedMap.this.get(k), (e.getValue()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(final Object o) {
      if (!(o instanceof Map.Entry))
        return false;
      final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;

      final K k = ((K) e.getKey());
      final V v = ((V) e.getValue());

      final int oldPos = KVArraySortedMap.this.findKey(k);
      if (oldPos == -1 || !java.util.Objects.equals(v, KVArraySortedMap.this.value[oldPos]))
        return false;
      final int tail = size - oldPos - 1;
      System.arraycopy(KVArraySortedMap.this.key, oldPos + 1, KVArraySortedMap.this.key, oldPos, tail);
      System.arraycopy(KVArraySortedMap.this.value, oldPos + 1, KVArraySortedMap.this.value, oldPos,
                       tail);
      KVArraySortedMap.this.size--;

      KVArraySortedMap.this.key[size] = null;

      KVArraySortedMap.this.value[size] = null;

      return true;
    }
  }

  @Override
  public FastEntrySet<K, V> object2ObjectEntrySet() {
    return new EntrySet();
  }

  private int findKey(final Object k) {
    return size == 0 ? -1 : Arrays.binarySearch(key, 0, size, k);
  }

  @Override
  @SuppressWarnings("unchecked")
  public V get(final Object k) {
    final int index = findKey(k);
    return index < 0 ? defRetValue : (V) value[index];
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public void clear() {
    Arrays.fill(key, null);
    Arrays.fill(value, null);
    size = 0;
  }

  @Override
  public boolean containsKey(final Object k) {
    return findKey(k) != -1;
  }

  @Override
  public boolean containsValue(Object v) {
    for (int i = size; i-- != 0;)
      if (java.util.Objects.equals(value[i], v))
        return true;
    return false;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public V put(K k, V v) {
    final int index = findKey(k);
    if (index >= 0) {
      final V oldValue = (V) value[index];
      value[index] = v;
      return oldValue;
    }
    final int x = -index - 1;
    if (size == key.length) {
      final int newSize = newSize(size);
      if (x == size) {
        key = Arrays.copyOf(key, newSize);
        value = Arrays.copyOf(value, newSize);
      } else {
        final Object[] newKey = new Object[newSize];
        final Object[] newValue = new Object[newSize];
        System.arraycopy(key, 0, newKey, 0, x);
        System.arraycopy(key, x, newKey, x + 1, size - x);
        System.arraycopy(value, 0, newValue, 0, x);
        System.arraycopy(value, x, newValue, x + 1, size - x);
        key = newKey;
        value = newValue;
      }
    } else if (x < size) {
      System.arraycopy(key, x, key, x + 1, size - x);
      System.arraycopy(value, x, value, x + 1, size - x);
    }
    key[x] = k;
    value[x] = v;
    size++;
    return defRetValue;
  }

  protected int newSize(int current)
  {
    return current == 0 ? 2 : current * 2;
  }

  @Override
  @SuppressWarnings("unchecked")
  public V remove(final Object k) {
    final int index = findKey(k);
    if (index < 0) {
      return defRetValue;
    }
    final V oldValue = (V) value[index];
    if (index < size - 1) {
      final int tail = size - index - 1;
      System.arraycopy(key, index + 1, key, index, tail);
      System.arraycopy(value, index + 1, value, index, tail);
    }
    size--;
    key[size] = null;
    value[size] = null;

    return oldValue;
  }

  @Override
  public ObjectSet<K> keySet() {
    return new AbstractObjectSet<K>() {
      @Override
      public boolean contains(final Object k) {
        return findKey(k) != -1;
      }

      @Override
      public boolean remove(final Object k) {
        final int oldPos = findKey(k);
        if (oldPos == -1)
          return false;
        final int tail = size - oldPos - 1;
        System.arraycopy(key, oldPos + 1, key, oldPos, tail);
        System.arraycopy(value, oldPos + 1, value, oldPos, tail);
        size--;
        return true;
      }

      @Override
      public ObjectIterator<K> iterator() {
        return new ObjectIterator<K>() {
          int pos = 0;
          @Override
          public boolean hasNext() {
            return pos < size;
          }

          @Override
          @SuppressWarnings("unchecked")
          public K next() {
            if (!hasNext())
              throw new NoSuchElementException();
            return (K) key[pos++];
          }

          @Override
          public void remove() {
            if (pos == 0)
              throw new IllegalStateException();
            final int tail = size - pos;
            System.arraycopy(key, pos, key, pos - 1, tail);
            System.arraycopy(value, pos, value, pos - 1, tail);
            size--;
          }
        };
      }

      @Override
      public int size() {
        return size;
      }

      @Override
      public void clear() {
        KVArraySortedMap.this.clear();
      }
    };
  }

  @Override
  public ObjectCollection<V> values() {
    return new AbstractObjectCollection<V>() {

      @Override
      public boolean contains(final Object v) {
        return containsValue(v);
      }

      @Override
      public it.unimi.dsi.fastutil.objects.ObjectIterator<V> iterator() {
        return new it.unimi.dsi.fastutil.objects.ObjectIterator<V>() {
          int pos = 0;
          @Override
          public boolean hasNext() {
            return pos < size;
          }

          @Override
          @SuppressWarnings("unchecked")
          public V next() {
            if (!hasNext())
              throw new NoSuchElementException();
            return (V) value[pos++];
          }

          @Override
          public void remove() {
            if (pos == 0)
              throw new IllegalStateException();
            final int tail = size - pos;
            System.arraycopy(key, pos, key, pos - 1, tail);
            System.arraycopy(value, pos, value, pos - 1, tail);
            size--;
          }
        };
      }

      @Override
      public int size() {
        return size;
      }

      @Override
      public void clear() {
        KVArraySortedMap.this.clear();
      }
    };
  }
}
