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

package io.druid.segment;

import io.druid.segment.data.IndexedInts;

import java.io.IOException;
import java.util.Set;

/**
 */
public interface IndexProvidingSelector extends DimensionSelector
{
  ColumnSelectorFactory wrapFactory(ColumnSelectorFactory factory);

  Set<String> targetColumns();

  default boolean hasFilter()
  {
    return false;
  }

  abstract class Delegated extends DelegatedDimensionSelector implements IndexProvidingSelector
  {
    public Delegated(DimensionSelector delegate)
    {
      super(delegate);
    }
  }

  class IndexHolder
  {
    volatile int version;
    volatile int index;

    int index()
    {
      return index;
    }

    IndexedInts indexed(final IndexedInts delegate)
    {
      version++;
      return new IndexedInts()
      {
        @Override
        public int size()
        {
          return delegate.size();
        }

        @Override
        public int get(int index)
        {
          IndexHolder.this.index = index;
          return delegate.get(index);
        }

        @Override
        public void close() throws IOException
        {
          delegate.close();
        }
      };
    }
  }
}
