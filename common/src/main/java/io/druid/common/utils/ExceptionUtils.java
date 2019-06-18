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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class ExceptionUtils
{
  public static List<String> stackTrace(Throwable e)
  {
    return stackTrace(e, Sets.<Throwable>newHashSet(), Lists.<String>newArrayList(), "");
  }

  public static List<String> stackTrace(Throwable e, Set<Throwable> visited, List<String> errorStack, String prefix)
  {
    StackTraceElement[] trace = e.getStackTrace();
    if (trace.length == 0) {
      return errorStack;
    }
    errorStack.add(prefix + trace[0]);
    if (trace.length == 1) {
      return errorStack;
    }
    for (StackTraceElement element : Arrays.copyOfRange(trace, 1, Math.min(12, trace.length))) {
      String stack = element.toString();
      if (errorStack.contains(stack)) {
        break;
      }
      errorStack.add(stack);
    }
    errorStack.add("... more");
    if (e.getCause() != null && visited.add(e.getCause())) {
      stackTrace(e.getCause(), visited, errorStack, "Caused by: ");
    }
    return errorStack;
  }
}
