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

package io.druid.segment.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import io.druid.common.guava.BinaryRef;
import io.druid.data.UTF8Bytes;
import io.druid.segment.bitmap.IntIterators;
import io.druid.segment.data.Dictionary;
import org.roaringbitmap.IntIterator;

public interface DictionaryMatcher
{
  default IntIterator wrap(Dictionary<String> dictionary, IntIterator iterator) {return iterator;}

  Predicate<String> matcher(Dictionary<String> dictionary);

  interface RawSupport extends DictionaryMatcher
  {
    Predicate<BinaryRef> rawMatcher(Dictionary<String> dictionary);
  }

  class WithPrefix implements DictionaryMatcher
  {
    final String prefix;
    final Predicate<String> predicate;

    boolean matched = true;

    public WithPrefix(String prefix, Predicate<String> predicate)
    {
      this.prefix = Preconditions.checkNotNull(prefix);
      this.predicate = Preconditions.checkNotNull(predicate);
    }

    @Override
    public IntIterator wrap(Dictionary<String> dictionary, IntIterator iterator)
    {
      if (dictionary == null || !dictionary.isSorted()) {
        return iterator;
      }
      final int index = dictionary.indexOf(prefix);
      final int sx = index < 0 ? -index - 1 : index;
      if (sx > 0) {
        iterator = iterator == null
                   ? IntIterators.fromTo(sx, dictionary.size())
                   : IntIterators.advanceTo(iterator, x -> x >= sx);
      }
      return new IntIterators.Delegated(iterator)
      {
        @Override
        public boolean hasNext()
        {
          return matched && super.hasNext();
        }
      };
    }

    @Override
    public Predicate<String> matcher(Dictionary<String> dictionary)
    {
      if (dictionary == null || !dictionary.isSorted()) {
        return v -> v.startsWith(prefix) && predicate.apply(v);
      }
      return v -> matched && (matched &= v.startsWith(prefix)) && predicate.apply(v);
    }
  }

  class WithRawPrefix extends WithPrefix implements RawSupport
  {
    private final UTF8Bytes prefix;
    private final Predicate<BinaryRef> predicate2;

    public WithRawPrefix(String prefix, Predicate<String> predicate1, Predicate<BinaryRef> predicate2)
    {
      super(prefix, predicate1);
      this.prefix = UTF8Bytes.of(prefix);
      this.predicate2 = Preconditions.checkNotNull(predicate2);
    }

    @Override
    public Predicate<BinaryRef> rawMatcher(Dictionary<String> dictionary)
    {
      if (dictionary == null || !dictionary.isSorted()) {
        return ref -> ref.startsWith(prefix) && predicate2.apply(ref);
      }
      return ref -> matched && (matched &= ref.startsWith(prefix)) && predicate2.apply(ref);
    }
  }
}
