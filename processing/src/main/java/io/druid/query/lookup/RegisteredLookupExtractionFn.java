/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.common.KeyBuilder;
import io.druid.query.extraction.ExtractionFn;

import javax.annotation.Nullable;

public class RegisteredLookupExtractionFn implements ExtractionFn
{
  // Protected for moving to not-null by `delegateLock`
  private volatile ExtractionFn delegate = null;
  private final Object delegateLock = new Object();
  private final LookupReferencesManager manager;
  private final String lookup;
  private final boolean retainMissingValue;
  private final String replaceMissingValueWith;
  private final boolean injective;
  private final boolean optimize;
  private final Integer keyNums;

  @JsonCreator
  public RegisteredLookupExtractionFn(
      @JacksonInject LookupReferencesManager manager,
      @JsonProperty("lookup") String lookup,
      @JsonProperty("retainMissingValue") final boolean retainMissingValue,
      @Nullable @JsonProperty("replaceMissingValueWith") final String replaceMissingValueWith,
      @JsonProperty("injective") final boolean injective,
      @JsonProperty("optimize") Boolean optimize,
      @JsonProperty("keyNums") Integer keyNums
  )
  {
    Preconditions.checkArgument(lookup != null, "`lookup` required");
    this.manager = manager;
    this.replaceMissingValueWith = replaceMissingValueWith;
    this.retainMissingValue = retainMissingValue;
    this.injective = injective;
    this.optimize = optimize == null ? true : optimize;
    this.lookup = lookup;
    this.keyNums = keyNums == null ? 1: keyNums;
  }

  @JsonProperty("lookup")
  public String getLookup()
  {
    return lookup;
  }

  @JsonProperty("retainMissingValue")
  public boolean isRetainMissingValue()
  {
    return retainMissingValue;
  }

  @JsonProperty("replaceMissingValueWith")
  public String getReplaceMissingValueWith()
  {
    return replaceMissingValueWith;
  }

  @JsonProperty("injective")
  public boolean isInjective()
  {
    return injective;
  }

  @JsonProperty("optimize")
  public boolean isOptimize()
  {
    return optimize;
  }

  @JsonProperty("keyNums")
  public Integer getKeyNums()
  {
    return keyNums;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(getClass().getCanonicalName())
                  .append(getLookup())
                  .append(ensureDelegate());
  }

  @Override
  public String apply(Object value)
  {
    return ensureDelegate().apply(value);
  }

  @Override
  public String apply(String value)
  {
    return ensureDelegate().apply(value);
  }

  @Override
  public String apply(long value)
  {
    return ensureDelegate().apply(value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return ensureDelegate().preservesOrdering();
  }

  @Override
  public boolean isOneToOne()
  {
    return ensureDelegate().isOneToOne();
  }

  private ExtractionFn ensureDelegate()
  {
    if (null == delegate) {
      // http://www.javamex.com/tutorials/double_checked_locking.shtml
      synchronized (delegateLock) {
        if (null == delegate) {
          delegate = (getKeyNums() == 1) ?
              new LookupExtractionFn(
                  Preconditions.checkNotNull(manager.get(getLookup()), "Lookup [%s] not found", getLookup()).get(),
                  isRetainMissingValue(),
                  getReplaceMissingValueWith(),
                  isInjective(),
                  isOptimize()
              )
              :
              new MultiDimLookupExtractionFn(
                  Preconditions.checkNotNull(manager.get(getLookup()), "Lookup [%s] not found", getLookup()).get(),
                  getReplaceMissingValueWith(),
                  isOptimize(),
                  getKeyNums()
              );
        }
      }
    }
    return delegate;
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

    RegisteredLookupExtractionFn that = (RegisteredLookupExtractionFn) o;

    if (isRetainMissingValue() != that.isRetainMissingValue()) {
      return false;
    }
    if (isInjective() != that.isInjective()) {
      return false;
    }
    if (isOptimize() != that.isOptimize()) {
      return false;
    }
    if (!getLookup().equals(that.getLookup())) {
      return false;
    }
    return getReplaceMissingValueWith() != null
           ? getReplaceMissingValueWith().equals(that.getReplaceMissingValueWith())
           : that.getReplaceMissingValueWith() == null;
  }

  @Override
  public int hashCode()
  {
    int result = getLookup().hashCode();
    result = 31 * result + (isRetainMissingValue() ? 1 : 0);
    result = 31 * result + (getReplaceMissingValueWith() != null ? getReplaceMissingValueWith().hashCode() : 0);
    result = 31 * result + (isInjective() ? 1 : 0);
    result = 31 * result + (isOptimize() ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "RegisteredLookupExtractionFn{" +
           "delegate=" + delegate +
           ", lookup='" + lookup + '\'' +
           ", retainMissingValue=" + retainMissingValue +
           ", replaceMissingValueWith='" + replaceMissingValueWith + '\'' +
           ", injective=" + injective +
           ", optimize=" + optimize +
           '}';
  }
}
