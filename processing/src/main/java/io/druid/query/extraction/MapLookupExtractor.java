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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.KeyBuilder;
import io.druid.query.lookup.LookupExtractor;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@JsonTypeName("map")
public class MapLookupExtractor extends LookupExtractor
{
  private final Map<Object, String> map;

  private final boolean isOneToOne;

  @JsonCreator
  public MapLookupExtractor(
      @JsonProperty("map") Map<Object, String> map,
      @JsonProperty("isOneToOne") boolean isOneToOne
  )
  {
    this.map = Preconditions.checkNotNull(map, "map");
    this.isOneToOne = isOneToOne;
  }

  @JsonProperty
  public Map<Object, String> getMap()
  {
    return ImmutableMap.copyOf(map);
  }

  @Nullable
  @Override
  public String apply(@NotNull Object val)
  {
    return map.get(val);
  }

  @Override
  public List<Object> unapply(final String value)
  {
    return Lists.newArrayList(Maps.filterKeys(map, new Predicate<Object>()
    {
      @Override public boolean apply(@Nullable Object key)
      {
        return map.get(key).equals(Strings.nullToEmpty(value));
      }
    }).keySet());

  }

  @Override
  @JsonProperty("isOneToOne")
  public boolean isOneToOne()
  {
    return isOneToOne;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    for (Map.Entry<Object, String> entry : map.entrySet()) {
      builder.append(entry.getKey()).sp().append(entry.getValue()).sp();
    }
    return builder;
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

    MapLookupExtractor that = (MapLookupExtractor) o;

    return map.equals(that.map);
  }

  @Override
  public int hashCode()
  {
    return map.hashCode();
  }

}
