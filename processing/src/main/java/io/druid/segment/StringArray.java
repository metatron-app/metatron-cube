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

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 */
public class StringArray extends ObjectArray<String>
{
  public static StringArray of(Object[] array, String nullValue)
  {
    final String[] strings = new String[array.length];
    for (int i = 0; i < array.length; i++) {
      strings[i] = Objects.toString(array[i], nullValue);
    }
    return new StringArray(strings);
  }

  public static StringArray of(String[] array)
  {
    return new StringArray(Preconditions.checkNotNull(array));
  }

  public StringArray(String[] array)
  {
    super(array);
  }
}
