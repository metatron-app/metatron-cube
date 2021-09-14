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

package io.druid.query.dimension;

import io.druid.data.ValueDesc;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

/**
 * Test dimension selector that has cardinality=26
 * encoding 0 -> a, 1 -> b, ...
 * row -> [c,e,g]
 */
class TestDimensionSelector implements DimensionSelector
{
  public final static TestDimensionSelector instance = new TestDimensionSelector();

  private TestDimensionSelector()
  {

  }

  @Override
  public IndexedInts getRow()
  {
    return IndexedInts.from(new int[]{2, 4, 6});
  }

  @Override
  public int getValueCardinality()
  {
    return 26;
  }

  @Override
  public Object lookupName(int id)
  {
    return String.valueOf((char) (id + 'a'));
  }

  @Override
  public ValueDesc type()
  {
    return ValueDesc.STRING;
  }

  @Override
  public int lookupId(Comparable name)
  {
    return ((String) name).charAt(0) - 'a';
  }

  @Override
  public boolean withSortedDictionary()
  {
    return true;
  }
}
