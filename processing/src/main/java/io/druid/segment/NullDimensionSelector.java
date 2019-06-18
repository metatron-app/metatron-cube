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

import com.google.common.collect.Iterators;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.segment.data.IndexedInts;

import java.io.IOException;
import java.util.Iterator;

public class NullDimensionSelector implements DimensionSelector
{
  public static final DimensionSelector STRING_TYPE = new NullDimensionSelector(ValueDesc.STRING);

  public static final IndexedInts SINGLETON = new IndexedInts()
  {
    @Override
    public int size()
    {
      return 1;
    }

    @Override
    public int get(int index)
    {
      return 0;
    }

    @Override
    public Iterator<Integer> iterator()
    {
      return Iterators.singletonIterator(0);
    }

    @Override
    public void fill(int index, int[] toFill)
    {
      throw new UnsupportedOperationException("NullDimensionSelector does not support fill");
    }

    @Override
    public void close() throws IOException
    {
    }
  };

  private final ValueDesc type;

  public NullDimensionSelector(ValueDesc type)
  {
    this.type = type;
  }

  @Override
  public IndexedInts getRow()
  {
    return SINGLETON;
  }

  @Override
  public int getValueCardinality()
  {
    return 1;
  }

  @Override
  public Comparable lookupName(int id)
  {
    return null;
  }

  @Override
  public ValueDesc type()
  {
    return type;
  }

  @Override
  public int lookupId(Comparable name)
  {
    return StringUtils.isNullOrEmpty(name) ? 0 : -1;
  }
}
