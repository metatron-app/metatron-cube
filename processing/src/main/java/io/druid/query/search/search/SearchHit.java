/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.search.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 */
public class SearchHit implements Comparable<SearchHit>
{
  public static final String DIMENSION = "$dimension";
  public static final String VALUE = "$value";
  public static final String COUNT = "$count";

  private final String dimension;
  private final String value;
  private final Integer count;

  @JsonCreator
  public SearchHit(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value,
      @JsonProperty("count") Integer count
  )
  {
    this.dimension = checkNotNull(dimension);
    this.value = checkNotNull(value);
    this.count = count;
  }

  public SearchHit(String dimension, String value)
  {
    this(dimension, value, null);
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @JsonProperty
  public Integer getCount()
  {
    return count;
  }

  @Override
  public int compareTo(SearchHit o)
  {
    int retVal = dimension.equals(o.dimension) ? 0 : dimension.compareTo(o.dimension);
    if (retVal == 0) {
      retVal = value.equals(o.value) ? 0 : value.compareTo(o.value);
    }
    return retVal;
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

    SearchHit searchHit = (SearchHit) o;

    return equals(searchHit);
  }

  public boolean equals(SearchHit searchHit)
  {
    if (this == searchHit) {
      return true;
    }
    if (!dimension.equals(searchHit.dimension)) {
      return false;
    }
    if (!value.equals(searchHit.value)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimension.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "Hit{" +
           "dimension='" + dimension + '\'' +
           ", value='" + value + '\'' +
           (count != null ? ", count=" + count : "") +
           '}';
  }
}
