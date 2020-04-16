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

package io.druid.query.lookup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.druid.common.KeyBuilder;
import io.druid.query.extraction.ExtractionCacheHelper;
import io.druid.query.extraction.MultiInputFunctionalExtraction;
import org.apache.commons.collections.keyvalue.MultiKey;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class MultiDimLookupExtractionFn extends MultiInputFunctionalExtraction
{
  private final LookupExtractor lookup;
  private final boolean optimize;
  private final Integer numKeys;

  @JsonCreator
  public MultiDimLookupExtractionFn(
      @JsonProperty("lookup") final LookupExtractor lookup,
      @Nullable @JsonProperty("replaceMissingValueWith") final String replaceMissingValueWith,
      @JsonProperty("optimize") final Boolean optimize,
      @JsonProperty("numKeys") final int numKeys
  )
  {
    super(
        new Function<List<String>, String>()
        {
          @Nullable
          @Override
          public String apply(List<String> inputList)
          {
            Preconditions.checkArgument(
                inputList.size() == numKeys,
                String.format("Number of Keys should be %d", numKeys)
            );
            String[] inputArray = new String[inputList.size()];
            inputArray = inputList.toArray(inputArray);
            MultiKey key = new MultiKey(inputArray);
            return lookup.apply(key);
          }
        },
        replaceMissingValueWith
    );
    this.lookup = lookup;
    this.optimize = optimize == null || optimize;
    Preconditions.checkArgument(numKeys > 1, "number of dimension keys should be greater than 1");
    this.numKeys = numKeys;
  }

  @JsonProperty
  public LookupExtractor getLookup()
  {
    return lookup;
  }

  @Override
  @JsonProperty
  public String getReplaceMissingValueWith()
  {
    return super.getReplaceMissingValueWith();
  }

  @JsonProperty("optimize")
  public boolean isOptimize()
  {
    return optimize;
  }

  @JsonProperty
  public int getNumKeys()
  {
    return numKeys;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(ExtractionCacheHelper.CACHE_TYPE_ID_MULTILOOKUP)
                  .append(lookup)
                  .append(getReplaceMissingValueWith())
                  .append(isOptimize())
                  .append(getNumKeys());
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

    MultiDimLookupExtractionFn that = (MultiDimLookupExtractionFn) o;

    if (optimize != that.optimize) {
      return false;
    }
    if (numKeys != that.numKeys) {
      return false;
    }
    if (!Objects.equals(lookup, that.lookup)) {
      return false;
    }
    return getReplaceMissingValueWith() != null
           ? getReplaceMissingValueWith().equals(that.getReplaceMissingValueWith())
           : that.getReplaceMissingValueWith() == null;

  }

  @Override
  public int hashCode()
  {
    int result = getLookup() != null ? getLookup().hashCode() : 0;
    result = 31 * result + (isOptimize() ? 1 : 0);
    result = 31 * result + (getReplaceMissingValueWith() != null ? getReplaceMissingValueWith().hashCode() : 0);
    result = 31 * result + numKeys;
    return result;
  }
}
