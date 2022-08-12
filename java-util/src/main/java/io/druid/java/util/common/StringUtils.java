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

package io.druid.java.util.common;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.IllegalFormatException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;

/**
 */
public class StringUtils
{
  // Charset parameters to String are currently slower than the charset's string name
  public static final Charset UTF8_CHARSET = Charsets.UTF_8;
  public static final String UTF8_STRING = com.google.common.base.Charsets.UTF_8.toString();

  private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
  private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

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

  public static String toUTF8String(byte[] bytes, int offset, int length)
  {
    try {
      return new String(bytes, offset, length, UTF8_STRING);
    }
    catch (UnsupportedEncodingException e) {
      // Should never happen
      throw Throwables.propagate(e);
    }
  }

  public static byte[] encodeBase64(byte[] input)
  {
    return BASE64_ENCODER.encode(input);
  }

  public static byte[] decodeBase64(byte[] input)
  {
    return BASE64_DECODER.decode(input);
  }

  public static byte[] decodeBase64(String input)
  {
    return BASE64_DECODER.decode(input);
  }

  @Nullable
  public static String urlDecode(String s)
  {
    if (s == null) {
      return null;
    }
    try {
      return URLDecoder.decode(s, "UTF-8");
    }
    catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
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
    return value == null || (value instanceof String && ((String)value).isEmpty());
  }

  public static Object emptyToNull(Object comparable)
  {
    return "".equals(comparable) ? null : comparable;
  }

  public static Comparable nullToEmpty(Comparable raw)
  {
    return raw == null ? "" : raw;
  }

  public static String toString(Object value, String nullValue)
  {
    return isNullOrEmpty(value) ? nullValue : Objects.toString(value, nullValue);
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
    Preconditions.checkArgument(value >= 0);
    int i = 0;
    while (value > 0x64000 && i < CODE.length) {
      value >>= 10;
      i++;
    }
    return String.format("%,d%s", value, CODE[i]);
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

  public static String limit(String text, int limit)
  {
    return text == null || text.length() < limit ? text : text.substring(limit) + "...";
  }

  public static String identifier(String string)
  {
    return quote(string, '"');
  }

  public static String quote(String string, char quote)
  {
    return quote + string + quote;
  }

  public static String fromUtf8(final byte[] bytes)
  {
    return fromUtf8(bytes, 0, bytes.length);
  }

  public static String fromUtf8(final byte[] bytes, int offset, int length)
  {
    try {
      return new String(bytes, offset, length, UTF8_STRING);
    }
    catch (UnsupportedEncodingException e) {
      // Should never happen
      throw Throwables.propagate(e);
    }
  }

  public static String fromUtf8(ByteBuffer buffer, int length)
  {
    final byte[] bytes = new byte[length];
    buffer.get(bytes);
    return fromUtf8(bytes);
  }

  public static String fromUtf8(ByteBuffer buffer, int offset, int length)
  {
    if (buffer.isDirect()) {
      return fromUtf8(copyDirect(buffer, offset, length));
    } else if (!buffer.isReadOnly()){
      return fromUtf8(buffer.array(), buffer.arrayOffset() + offset, length);
    } else {
      final byte[] bytes = new byte[length];
      buffer.position(offset);
      buffer.get(bytes);
      return fromUtf8(bytes);
    }
  }

  public static String fromUtf8(final ByteBuffer buffer)
  {
    return fromUtf8(buffer, 0, buffer.remaining());
  }

  public static byte[] toUtf8(final String string)
  {
    try {
      return string.getBytes(UTF8_STRING);
    }
    catch (UnsupportedEncodingException e) {
      // Should never happen
      throw Throwables.propagate(e);
    }
  }

  private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

  private static byte[] copyDirect(ByteBuffer buffer, int offset, int length)
  {
    Preconditions.checkArgument(buffer.isDirect());
    if ((offset | length | offset + length) < 0) {
      throw new IndexOutOfBoundsException();
    }
    if (length > buffer.remaining()) {
      throw new BufferUnderflowException();
    }
    final Unsafe unsafe = (Unsafe) UnsafeUtils.theUnsafe();
    final byte[] dest = new byte[length];
    long soffset = ((DirectBuffer) buffer).address() + offset;
    while (length > 0) {
      long size = length > UNSAFE_COPY_THRESHOLD ? UNSAFE_COPY_THRESHOLD : length;
      unsafe.copyMemory(null, soffset, dest, Unsafe.ARRAY_BYTE_BASE_OFFSET, size);
      length -= size;
      soffset += size;
      offset += size;
    }
    return dest;
  }

  public static String safeFormat(String message, Object... formatArgs)
  {
    if (formatArgs == null || formatArgs.length == 0) {
      return message;
    }
    try {
      return String.format(message, formatArgs);
    }
    catch (IllegalFormatException e) {
      StringBuilder bob = new StringBuilder(message);
      for (Object formatArg : formatArgs) {
        bob.append("; ").append(formatArg);
      }
      return bob.toString();
    }
  }
}
