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

package io.druid.query.extraction;

import com.google.common.base.Strings;
import io.druid.common.KeyBuilder;

import java.util.Objects;

public class IdentityExtractionFn implements ExtractionFn
{
  private static final IdentityExtractionFn emptyToNull = new IdentityExtractionFn();
  private static final IdentityExtractionFn nullToEmpty = new IdentityExtractionFn() {
    @Override
    public KeyBuilder getCacheKey(KeyBuilder builder)
    {
      return builder.append(ExtractionCacheHelper.CACHE_TYPE_ID_NULL_TO_EMPTY);
    }

    @Override
    public String apply(Object value)
    {
      return Objects.toString(value, "");
    }

    @Override
    public String apply(String value)
    {
      return Strings.nullToEmpty(value);
    }
  };

  private IdentityExtractionFn()
  {

  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(ExtractionCacheHelper.CACHE_TYPE_ID_IDENTITY);
  }

  @Override
  public String apply(String value)
  {
    return Strings.emptyToNull(value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return true;
  }

  @Override
  public boolean isOneToOne()
  {
    return true;
  }

  @Override
  public String toString()
  {
     return "Identity";
  }

  @Override
  public boolean equals(Object o)
  {
     return o instanceof IdentityExtractionFn;
  }

  public static IdentityExtractionFn getInstance()
  {
    return emptyToNull;
  }

  public static IdentityExtractionFn nullToEmpty()
  {
    return nullToEmpty;
  }
}
