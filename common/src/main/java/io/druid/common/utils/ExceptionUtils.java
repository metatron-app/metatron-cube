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
