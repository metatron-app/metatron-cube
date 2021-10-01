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

package io.druid.data;

import com.google.common.primitives.UnsignedBytes;
import io.druid.common.guava.Comparators;
import io.druid.common.utils.StringUtils;

import java.util.Arrays;
import java.util.Comparator;

public final class UTF8Bytes implements Comparable<UTF8Bytes>
{
  public static UTF8Bytes of(byte[] value)
  {
    return new UTF8Bytes(value);
  }

  public static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();
  public static final Comparator<byte[]> COMPARATOR_NF = Comparators.NULL_FIRST(COMPARATOR);

  private final byte[] value;

  private UTF8Bytes(byte[] value) {this.value = value;}

  public byte[] getValue()
  {
    return value;
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(value);
  }

  @Override
  public boolean equals(Object o)
  {
    return o instanceof UTF8Bytes && compareTo((UTF8Bytes) o) == 0;
  }

  @Override
  public String toString()
  {
    return value.length == 0 ? null : StringUtils.fromUtf8(value);
  }

  @Override
  public int compareTo(UTF8Bytes o)
  {
    return COMPARATOR.compare(value, o.value);
  }
}
