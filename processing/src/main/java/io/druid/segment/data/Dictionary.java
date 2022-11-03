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

package io.druid.segment.data;

import io.druid.common.guava.BinaryRef;
import io.druid.segment.Tools;

import java.util.List;
import java.util.function.ToIntBiFunction;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.druid.segment.data.DictionaryCompareOp.EQ;
import static io.druid.segment.data.DictionaryCompareOp.GT;
import static io.druid.segment.data.DictionaryCompareOp.GTE;
import static io.druid.segment.data.DictionaryCompareOp.LT;
import static io.druid.segment.data.DictionaryCompareOp.LTE;
import static io.druid.segment.data.DictionaryCompareOp.NEQ;

// common interface of non-compressed(GenericIndexed) and compressed dictionary
public interface Dictionary<T> extends Indexed.BufferBacked<T>
{
  int flag();

  default boolean isSorted()
  {
    return GenericIndexed.Feature.SORTED.isSet(flag());
  }

  Boolean containsNull();     // null for unknown

  @Override
  default int indexOf(T value)
  {
    return indexOf(value, 0, size(), true);
  }

  int indexOf(T value, int start, int end, boolean binary);

  default IntStream indexOf(List<T> values)
  {
    return indexOf(values.stream()).filter(x -> x >= 0);
  }

  default IntStream indexOf(Stream<T> stream)
  {
    return search(stream, (v, s) -> indexOf(v, s, size(), true));
  }

  default IntStream indexOfRaw(Stream<BinaryRef> stream)
  {
    return search(stream, (v, s) -> indexOf(v, s, size(), true));
  }

  int indexOf(BinaryRef bytes, int start, int end, boolean binary);

  byte[] getAsRaw(int index);

  long getSerializedSize();

  void scan(Tools.Scanner scanner);

  <R> Stream<R> stream(Tools.Function<R> scanner);

  void apply(int index, Tools.Scanner scanner);

  <R> R apply(int index, Tools.Function<R> function);

  void close();

  // for searching stream of sorted value
  public static <F> IntStream search(Stream<F> stream, ToIntBiFunction<F, Integer> function)
  {
    return search(stream, function, 0);
  }

  public static <F> IntStream search(Stream<F> stream, ToIntBiFunction<F, Integer> function, int start)
  {
    return stream.mapToInt(new ToIntFunction<F>()
    {
      private int p = start;

      @Override
      public int applyAsInt(F input)
      {
        return p = function.applyAsInt(input, p < 0 ? -p - 1 : p);
      }
    });
  }

  public static DictionaryCompareOp compareOp(String name)
  {
    switch (name) {
      case "==": return EQ;
      case "!=": return NEQ;
      case ">":  return GT;
      case ">=": return GTE;
      case "<":  return LT;
      case "<=": return LTE;
    }
    return null;
  }
}
