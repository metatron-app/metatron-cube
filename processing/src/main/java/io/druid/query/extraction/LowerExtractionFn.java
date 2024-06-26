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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Strings;
import io.druid.common.KeyBuilder;

import javax.annotation.Nullable;
import java.util.Locale;

@JsonTypeName("lower")
public class LowerExtractionFn implements ExtractionFn
{
  private final Locale locale;

  @JsonProperty
  private final String localeString;

  public LowerExtractionFn(@JsonProperty("locale") String localeString)
  {
    this.localeString = localeString;
    this.locale = localeString == null ? Locale.getDefault() : Locale.forLanguageTag(localeString);
  }

  /**
   * @param key string input of extraction function
   *
   * @return new string with all of the characters in {@code key} as an lower case  or <tt>null</tt> if {@code key} is empty or null
   */

  @Nullable
  @Override
  public String apply(String key)
  {
    if (Strings.isNullOrEmpty(key)) {
      return null;
    }
    return key.toLowerCase(locale);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(ExtractionCacheHelper.CACHE_TYPE_ID_LOWER)
                  .append(localeString);
  }
}
