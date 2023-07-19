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

import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.data.IndexedInts;

import java.util.Objects;

public class NullDimensionSelector implements DimensionSelector
{
  static DimensionSelector of(ValueDesc desc, ExtractionFn extractionFn)
  {
    if (extractionFn == null || StringUtils.isNullOrEmpty(extractionFn.apply(null))) {
      return new NullDimensionSelector(desc);
    }
    return new NullDimensionSelector(desc)
    {
      private final Object value = extractionFn.apply(null);

      @Override
      public Object lookupName(int id)
      {
        return value;
      }

      @Override
      public int lookupId(Object name)
      {
        return Objects.equals(value, name) ? 0 : -1;
      }
    };
  }
  public static final DimensionSelector STRING_TYPE = new NullDimensionSelector(ValueDesc.STRING);

  public static final IndexedInts SINGLETON = IndexedInts.from(0);

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
  public Object lookupName(int id)
  {
    return null;
  }

  @Override
  public ValueDesc type()
  {
    return type;
  }

  @Override
  public int lookupId(Object name)
  {
    return StringUtils.isNullOrEmpty(name) ? 0 : -1;
  }

  @Override
  public boolean withSortedDictionary()
  {
    return true;
  }
}
