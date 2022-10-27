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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.data.input.BytesOutputStream;

import java.security.MessageDigest;
import java.util.List;
import java.util.Set;

public class ExceptionUtils
{
  public static List<String> stackTrace(Throwable e)
  {
    List<String> errorStack = Lists.newArrayList();
    StackTraceElement[] trace = e.getStackTrace();
    for (StackTraceElement element : trace) {
      errorStack.add(element.toString());
    }
    Throwable cause = e.getCause();
    if (cause != null) {
      printEnclosedStackTrace(cause, trace, Sets.newHashSet(cause), errorStack);
    }
    return errorStack;
  }

  private static void printEnclosedStackTrace(
      Throwable t,
      StackTraceElement[] enclosing,
      Set<Throwable> visited,
      List<String> errorStack
  )
  {
    StackTraceElement[] trace = t.getStackTrace();
    int m = trace.length - 1;
    int n = enclosing.length - 1;
    while (m >= 0 && n >= 0 && trace[m].equals(enclosing[n])) {
      m--;n--;
    }
    int framesInCommon = trace.length - 1 - m;

    errorStack.add("Caused by: " + t);
    for (int i = 0; i <= m; i++) {
      errorStack.add(trace[i].toString());
    }
    if (framesInCommon != 0) {
      errorStack.add("... " + framesInCommon + " more");
    }
    Throwable cause = t.getCause();
    if (cause != null && visited.add(cause)) {
      printEnclosedStackTrace(cause, trace, visited, errorStack);
    }
  }

  public static <T extends Exception> T find(Throwable t, Class<T> find)
  {
    for (Throwable current = t; current != null; current = current.getCause()) {
      if (find.isInstance(current)) {
        return find.cast(current);
      }
    }
    return null;
  }

  public static byte[] digest(Throwable e, MessageDigest digest)
  {
    BytesOutputStream stream = collectHash(e, new BytesOutputStream());
    digest.update(stream.unwrap(), 0, stream.size());
    return digest.digest();
  }

  private static BytesOutputStream collectHash(Throwable e, BytesOutputStream stream)
  {
    stream.writeUTF(e.getMessage());
    StackTraceElement[] trace = e.getStackTrace();
    for (StackTraceElement element : trace) {
      stream.writeInt(element.hashCode());
    }
    Throwable cause = e.getCause();
    if (cause != null) {
      collectHash(cause, stream);
    }
    return stream;
  }
}
