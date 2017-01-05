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

package io.druid.common.utils;

import com.google.common.base.Function;
import com.google.common.base.Strings;

/**
 */
public class StringUtils extends com.metamx.common.StringUtils
{
  public static final Function<String, String> NULL_TO_EMPTY = new Function<String, String>()
  {
    @Override
    public String apply(String s)
    {
      return Strings.nullToEmpty(s);
    }
  };

  private static final byte[] EMPTY_BYTES = new byte[0];

  // should be used only for estimation
  // returns the same result with StringUtils.fromUtf8(value).length for valid string values
  // does not check validity of format and returns over-estimated result for invalid string (see UT)
  public static int estimatedBinaryLengthAsUTF8(String value)
  {
    int length = 0;
    for (int i = 0; i < value.length(); i++) {
      char var10 = value.charAt(i);
      if (var10 < 0x80) {
        length += 1;
      } else if (var10 < 0x800) {
        length += 2;
      } else if (Character.isSurrogate(var10)) {
        length += 4;
        i++;
      } else {
        length += 3;
      }
    }
    return length;
  }

  public static byte[] toUtf8WithNullToEmpty(final String string)
  {
    return string == null ? EMPTY_BYTES : toUtf8(string);
  }

  public static boolean isNullOrEmpty(String value)
  {
    return value == null || value.isEmpty();
  }

  public static long parseKMGT(String value)
  {
    return parseKMGT(value, 0);
  }

  public static long parseKMGT(String value, long defaultValue)
  {
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    value = value.replaceAll(",", "").replaceAll("_", "").trim();
    int index = 0;
    for (char x : value.toCharArray()) {
      if (x != '-' && x != '+' && !Character.isDigit(x)) {
        long longValue = Long.parseLong(value.substring(0, index));
        String remain = value.substring(index).trim().toLowerCase();
        if (remain.startsWith("k")) {
          longValue <<= 10;
        } else if (remain.startsWith("m")) {
          longValue <<= 20;
        } else if (remain.startsWith("g")) {
          longValue <<= 30;
        } else if (remain.startsWith("t")) {
          longValue <<= 40;
        } else if (!remain.isEmpty()) {
          throw new IllegalArgumentException("Invalid unit " + remain);
        }
        return longValue;
      }
      index++;
    }
    return Long.parseLong(value.substring(0, index));
  }
}
