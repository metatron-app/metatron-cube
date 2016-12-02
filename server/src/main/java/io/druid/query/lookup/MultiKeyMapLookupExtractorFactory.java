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

package io.druid.query.lookup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.extraction.MultiKeyMapLookupExtractor;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

@JsonTypeName("multiKeyMap")
public class MultiKeyMapLookupExtractorFactory implements LookupExtractorFactory
{
  @JsonProperty
  private final List<List<String>> keyValueList;
  @JsonProperty
  private final boolean isOneToOne;
  private final MapLookupExtractor lookupExtractor;
  private final LookupIntrospectHandler lookupIntrospectHandler;

  @JsonCreator
  public MultiKeyMapLookupExtractorFactory(
      @JsonProperty("keyValueList") List<List<String>> keyValueList,
      @JsonProperty("isOneToOne") boolean isOneToOne
  )
  {
    this.keyValueList = Preconditions.checkNotNull(keyValueList, "map cannot be null");
    this.isOneToOne = isOneToOne;
    this.lookupExtractor = new MultiKeyMapLookupExtractor(keyValueList, isOneToOne);
    this.lookupIntrospectHandler = new MultiKeyMapLookupIntrospectionHandler(this.keyValueList);
  }

  @Override
  public boolean start()
  {
    return true;
  }

  @Override
  public boolean close()
  {
    return true;
  }

  /**
   * For MapLookups, the replaces consideration is very easy, it simply considers if the other is the same as this one
   *
   * @param other Some other LookupExtractorFactory which might need replaced
   *
   * @return true - should replace,   false - should not replace
   */
  @Override
  public boolean replaces(@Nullable LookupExtractorFactory other)
  {
    return !equals(other);
  }

  @Nullable
  @Override
  public LookupIntrospectHandler getIntrospectHandler()
  {
    return lookupIntrospectHandler;
  }

  @Override
  public LookupExtractor get()
  {
    return lookupExtractor;
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

    MultiKeyMapLookupExtractorFactory that = (MultiKeyMapLookupExtractorFactory) o;

    if (isOneToOne != that.isOneToOne) {
      return false;
    }
    if (keyValueList.size() != that.keyValueList.size()) {
      return false;
    }
    for (int index = 0; index < keyValueList.size(); index++) {
      List<String> keyValue = keyValueList.get(index);
      List<String> otherKV = that.keyValueList.get(index);
      if (keyValue.size() != otherKV.size()) {
        return false;
      }
      for (int kvIdx = 0; kvIdx < keyValue.size(); kvIdx++) {
        if (!keyValue.get(kvIdx).equals(otherKV.get(kvIdx))) {
          return false;
        }
      }
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = keyValueList.hashCode();
    result = 31 * result + (isOneToOne ? 1 : 0);
    return result;
  }

  public static class MultiKeyMapLookupIntrospectionHandler implements LookupIntrospectHandler
  {
    private final List<List<String>> keyValueList;
    public MultiKeyMapLookupIntrospectionHandler(List<List<String>> keyValueList)
    {
      this.keyValueList = keyValueList;
    }

    @GET
    @Path("/keys")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getKeys()
    {
      return Response.ok(
          Lists.transform(
              keyValueList,
              new Function<List<String>, List<String>>()
              {
                @Override
                public List<String> apply(List<String> input)
                {
                  Preconditions.checkArgument(input.size() > 1, "At least one key is needed but %d", input.size() - 1);
                  return input.subList(0, input.size() - 1);
                }
              }
          )
      ).build();
    }

    @GET
    @Path("/values")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getValues()
    {
      return Response.ok(
          Lists.transform(
              keyValueList,
              new Function<List<String>, String>()
              {
                @Override
                public String apply(List<String> input)
                {
                  Preconditions.checkArgument(input.size() > 1, "At least one key is needed but %d", input.size() - 1);
                  return input.get(input.size() - 1);
                }
              }
          )
      ).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getMap()
    {return Response.ok(keyValueList).build();}
  }
}
