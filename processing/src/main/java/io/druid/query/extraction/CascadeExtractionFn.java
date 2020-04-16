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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;

import java.util.List;

public class CascadeExtractionFn implements ExtractionFn
{
  private final List<ExtractionFn> extractionFns;
  private final ChainedExtractionFn chainedExtractionFn;

  @JsonCreator
  public CascadeExtractionFn(
      @JsonProperty("extractionFns") List<ExtractionFn> extractionFns
  )
  {
    Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(extractionFns), "extractionFns should not be null or empty");
    this.extractionFns = extractionFns;
    ChainedExtractionFn root = null;
    for (ExtractionFn fn : extractionFns) {
      root = new ChainedExtractionFn(Preconditions.checkNotNull(fn, "null function is not allowed"), root);
    }
    this.chainedExtractionFn = root;
  }

  @JsonProperty
  public List<ExtractionFn> getExtractionFns()
  {
    return extractionFns;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(ExtractionCacheHelper.CACHE_TYPE_ID_CASCADE)
                  .append(chainedExtractionFn);
  }

  @Override
  public String apply(Object value)
  {
    return chainedExtractionFn.apply(value);
  }

  @Override
  public String apply(String value)
  {
    return chainedExtractionFn.apply(value);
  }

  @Override
  public String apply(long value)
  {
    return chainedExtractionFn.apply(value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return chainedExtractionFn.preservesOrdering();
  }

  @Override
  public boolean isOneToOne()
  {
    return chainedExtractionFn.isOneToOne();
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

    CascadeExtractionFn that = (CascadeExtractionFn) o;

    if (!extractionFns.equals(that.extractionFns)) {
      return false;
    }
    if (!chainedExtractionFn.equals(that.chainedExtractionFn)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return chainedExtractionFn.hashCode();
  }

  @Override
  public String toString()
  {
    return "CascadeExtractionFn{" +
           "extractionFns=[" + chainedExtractionFn.toString() + "]}";
  }

  private static class ChainedExtractionFn
  {
    private final ExtractionFn fn;
    private final ChainedExtractionFn child;

    private ChainedExtractionFn(ExtractionFn fn, ChainedExtractionFn child)
    {
      this.fn = fn;
      this.child = child;
    }

    private KeyBuilder getCacheKey(KeyBuilder builder)
    {
      return builder.append(fn).append(child);
    }

    private String apply(Object value)
    {
      return fn.apply(child != null ? child.apply(value) : value);
    }

    private String apply(String value)
    {
      return fn.apply(child != null ? child.apply(value) : value);
    }

    private String apply(long value)
    {
      return fn.apply(child != null ? child.apply(value) : value);
    }

    private boolean preservesOrdering()
    {
      boolean childPreservesOrdering = child == null || child.preservesOrdering();
      return fn.preservesOrdering() && childPreservesOrdering;
    }

    private boolean isOneToOne()
    {
      return (child == null || child.isOneToOne()) && fn.isOneToOne();
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

      ChainedExtractionFn that = (ChainedExtractionFn) o;

      if (!fn.equals(that.fn)) {
        return false;
      }
      if (child != null && !child.equals(that.child)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = fn.hashCode();
      if (child != null) {
        result = 31 * result + child.hashCode();
      }
      return result;
    }

    @Override
    public String toString()
    {
      return (child != null)
             ? Joiner.on(",").join(child.toString(), fn.toString())
             : fn.toString();
    }
  }
}
