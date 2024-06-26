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

package io.druid.common.utils;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.druid.common.guava.BinaryRef;
import io.druid.common.guava.BytesRef;
import io.druid.data.UTF8Bytes;
import io.druid.data.ValueType;
import io.druid.data.input.BytesOutputStream;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.IllegalFormatException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;

/**
 */
public class StringUtils extends io.druid.java.util.common.StringUtils
{
  public static final Function<String, String> NULL_TO_EMPTY = new Function<String, String>()
  {
    @Override
    public String apply(String s)
    {
      return Strings.nullToEmpty(s);
    }
  };

  public static final Function<String, String> TO_UPPER = new Function<String, String>()
  {
    @Override
    public String apply(String s)
    {
      return s.toUpperCase();
    }
  };

  public static final Function<String, String> TO_LOWER = new Function<String, String>()
  {
    @Override
    public String apply(String s)
    {
      return s.toLowerCase();
    }
  };

  public static final byte[] EMPTY_BYTES = new byte[0];

  @Nullable
  public static byte[] nullableToUtf8(@Nullable final String string)
  {
    try {
      return string == null ? null : string.getBytes(UTF8_STRING);
    }
    catch (UnsupportedEncodingException e) {
      // Should never happen
      throw Throwables.propagate(e);
    }
  }

  public static String fromUtf8(ByteBuffer buffer, int length, BytesOutputStream scratch)
  {
    scratch.clear();
    scratch.ensureCapacity(length);
    buffer.get(scratch.unwrap(), 0, length);
    return fromUtf8(scratch.unwrap(), 0, length);
  }

  public static String fromUtf8(BinaryRef ref, BytesOutputStream scratch)
  {
    scratch.clear();
    scratch.ensureCapacity(ref.length());
    return fromUtf8(ref.copyTo(scratch).unwrap(), 0, ref.length());
  }

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

  public static URL toURL(String address)
  {
    try {
      return new URL(address);
    }
    catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  public static Function<Object, byte[]> serializer(final ValueType type)
  {
    return v -> v == null ? EMPTY_BYTES : type.toBytes(v);
  }

  public static byte[] toUtf8WithNullToEmpty(final Object string)
  {
    return toUtf8WithNullToEmpty(Objects.toString(string, null));
  }

  public static byte[] toUtf8WithNullToEmpty(final String string)
  {
    return string == null ? EMPTY_BYTES : toUtf8(string);
  }

  public static boolean isNullOrEmpty(String value)
  {
    return value == null || value.isEmpty();
  }

  public static boolean isNullOrEmpty(Object value)
  {
    return value == null || (value instanceof String && ((String) value).isEmpty());
  }

  public static Object emptyToNull(Object comparable)
  {
    return "".equals(comparable) ? null : comparable;
  }

  public static Object nullToEmpty(Object raw)
  {
    return raw == null ? "" : raw;
  }

  public static String toString(Object value, String nullValue)
  {
    return isNullOrEmpty(value) ? nullValue : Objects.toString(value, nullValue);
  }

  public static String unquote(String string)
  {
    if (string == null || string.length() < 2) {
      return string;
    }
    char char0 = string.charAt(0);
    if (char0 != '\'' && char0 != '\"') {
      return string;
    }
    char charX = string.charAt(string.length() - 1);
    if (char0 == charX) {
      return string.substring(1, string.length() - 1);
    }
    return string;
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

  private static final String[] CODE = new String[]{"B", "KB", "MB", "GB", "TB", "PB"};

  public static String toKMGT(long value)
  {
    boolean minus = value < 0;
    value = Math.abs(value);
    int i = 0;
    while (value > 0x64000 && i < CODE.length) {
      value >>= 10;
      i++;
    }
    return String.format("%,d%s", minus ? -value : value, CODE[i]);
  }

  public static byte[] concat(byte[]... array)
  {
    int length = 0;
    for (byte[] bytes : array) {
      length += bytes.length;
    }
    int i = 0;
    byte[] concat = new byte[length];
    for (byte[] bytes : array) {
      System.arraycopy(bytes, 0, concat, i, bytes.length);
      i += bytes.length;
    }
    return concat;
  }

  public static String concat(String delimiter, String... strings)
  {
    if (strings.length == 0) {
      return "";
    }
    if (strings.length == 1) {
      return Strings.nullToEmpty(strings[0]);
    }
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < strings.length; i++) {
      if (i > 0) {
        builder.append(delimiter);
      }
      builder.append(Strings.nullToEmpty(strings[i]));
    }
    return builder.toString();
  }

  public static String concat(String delimiter, Iterable<String> strings)
  {
    Iterator<String> iterator = strings.iterator();
    if (!iterator.hasNext()) {
      return "";
    }
    int i = 0;
    StringBuilder builder = new StringBuilder();
    while (iterator.hasNext()) {
      if (i++ > 0) {
        builder.append(delimiter);
      }
      builder.append(Strings.nullToEmpty(iterator.next()));
    }
    return builder.toString();
  }

  /**
   * Equivalent of String.format(Locale.ENGLISH, message, formatArgs).
   */
  public static String format(String message, Object... formatArgs)
  {
    return String.format(Locale.ENGLISH, message, formatArgs);
  }

  /**
   * Formats the string as {@link #format(String, Object...)}, but instead of failing on illegal format, returns the
   * concatenated format string and format arguments. Should be used for unimportant formatting like logging,
   * exception messages, typically not directly.
   */
  public static String nonStrictFormat(String message, Object... formatArgs)
  {
    if (formatArgs == null || formatArgs.length == 0) {
      return message;
    }
    try {
      return String.format(Locale.ENGLISH, message, formatArgs);
    }
    catch (IllegalFormatException e) {
      StringBuilder bob = new StringBuilder(message);
      for (Object formatArg : formatArgs) {
        bob.append("; ").append(formatArg);
      }
      return bob.toString();
    }
  }

  public static String toLowerCase(String s)
  {
    return s.toLowerCase(Locale.ENGLISH);
  }

  public static String toUpperCase(String s)
  {
    return s.toUpperCase(Locale.ENGLISH);
  }

  public static String removeChar(String s, char c)
  {
    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) == c) {
        return removeChar(s, c, i);
      }
    }
    return s;
  }

  private static String removeChar(String s, char c, int firstOccurranceIndex)
  {
    StringBuilder sb = new StringBuilder(s.length() - 1);
    sb.append(s, 0, firstOccurranceIndex);
    for (int i = firstOccurranceIndex + 1; i < s.length(); i++) {
      char charOfString = s.charAt(i);
      if (charOfString != c) {
        sb.append(charOfString);
      }
    }
    return sb.toString();
  }

  public static String substring(String text, int length)
  {
    return text == null || text.length() < length ? text : text.substring(0, length);
  }

  private static final int DEFAULT_LOG_LIMIT = 32;

  public static String forLog(String text)
  {
    return forLog(text, DEFAULT_LOG_LIMIT);
  }

  public static String forLog(String text, int limit)
  {
    if (text == null || text.length() <= limit) {
      return text;
    }
    int prefix = (int) (limit * 0.8);
    int postfix = text.length() - prefix;
    return String.format(
        "%s (..%d more..) %s", text.substring(0, prefix).trim(), text.length() - limit, text.substring(postfix).trim()
    );
  }

  public static String limit(String text, int limit)
  {
    return text == null || text.length() < limit ? text : text.substring(0, limit) + "...";
  }

  public static String identifier(String string)
  {
    return quote(string, '"');
  }

  public static String quote(String string, char quote)
  {
    return quote + string + quote;
  }

  public static boolean isSimpleIdentifier(String s)
  {
    for (int i = 0; i < s.length(); i++) {
      final char ch = s.charAt(i);
      if (ch != '_' && (ch >>> 8 != 0 || !Character.isLetterOrDigit(ch))) {
        return false;
      }
    }
    return true;
  }

  public static String replaceChar(String s, char c, String replacement)
  {
    int pos = s.indexOf(c);
    if (pos < 0) {
      return s;
    }
    StringBuilder sb = new StringBuilder(s.length() - 1 + replacement.length());
    int prevPos = 0;
    do {
      sb.append(s, prevPos, pos);
      sb.append(replacement);
      prevPos = pos + 1;
      pos = s.indexOf(c, pos + 1);
    } while (pos > 0);
    sb.append(s, prevPos, s.length());
    return sb.toString();
  }

  public static Iterable<String> splitAndTrim(String value, char separator)
  {
    Splitter splitter = Splitter.on(separator).trimResults();
    return Lists.newArrayList(splitter.split(value));
  }

  public static BytesRef stringAsRef(Object value)
  {
    if (value == null) {
      return BytesRef.EMPTY;
    } else if (value instanceof UTF8Bytes) {
      return ((UTF8Bytes) value).asRef();
    } else {
      return BytesRef.of(Objects.toString(value));
    }
  }

  private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

  public static String bytesToHex(byte[] bytes)
  {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < bytes.length; i++) {
      int v = bytes[i] & 0xFF;
      builder.append(HEX_ARRAY[v >>> 4]);
      builder.append(HEX_ARRAY[v & 0xf]);
      if (i >= bytes.length - 1) {
        continue;
      }
      if ((i + 1) % 32 == 0) {
        builder.append('\n');
        continue;
      }
      if ((i + 1) % 8 == 0) {
        builder.append(' ');
      }
      builder.append(' ');
    }
    return builder.toString();
  }
}
