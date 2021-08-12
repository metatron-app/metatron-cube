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

import io.druid.data.ValueDesc;
import io.druid.segment.data.IndexedInts;

/**
 */
public class DelegatedDimensionSelector implements DimensionSelector
{
  protected final DimensionSelector delegate;

  public DelegatedDimensionSelector(DimensionSelector delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public IndexedInts getRow()
  {
    return delegate.getRow();
  }

  @Override
  public int getValueCardinality()
  {
    return delegate.getValueCardinality();
  }

  @Override
  public Object lookupName(int id)
  {
    return delegate.lookupName(id);
  }

  @Override
  public ValueDesc type()
  {
    return delegate.type();
  }

  @Override
  public int lookupId(Comparable name)
  {
    return delegate.lookupId(name);
  }

  @Override
  public boolean withSortedDictionary()
  {
    return delegate.withSortedDictionary();
  }
}
