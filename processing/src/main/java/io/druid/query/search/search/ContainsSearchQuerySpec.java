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

package io.druid.query.search.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import io.druid.common.KeyBuilder;
import org.apache.commons.lang.StringUtils;

/**
 */
public class ContainsSearchQuerySpec implements SearchQuerySpec
{

  private static final byte CACHE_TYPE_ID = 0x1;

  private final String value;
  private final boolean caseSensitive;

  @JsonCreator
  public ContainsSearchQuerySpec(
      @JsonProperty("value") String value,
      @JsonProperty("caseSensitive") boolean caseSensitive
  )
  {
    this.value = value;
    this.caseSensitive = caseSensitive;
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @JsonProperty
  public boolean isCaseSensitive()
  {
    return caseSensitive;
  }

  @Override
  public boolean accept(String dimVal)
  {
    if (dimVal == null || value == null) {
      return false;
    }
    if (caseSensitive) {
      return dimVal.contains(value);
    }
    return StringUtils.containsIgnoreCase(dimVal, value);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(caseSensitive)
                  .append(value);
  }

  @Override
  public String toString()
  {
    return "ContainsSearchQuerySpec{" +
           "value=" + value + ", caseSensitive=" + caseSensitive +
           "}";
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ContainsSearchQuerySpec that = (ContainsSearchQuerySpec) o;

    if (caseSensitive ^ that.caseSensitive) {
      return false;
    }

    if (value == null && that.value == null) {
      return true;
    }

    return value != null && value.equals(that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(value) + (caseSensitive ? (byte) 1 : 0);
  }
}
