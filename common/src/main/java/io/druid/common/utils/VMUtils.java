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

import com.sun.management.HotSpotDiagnosticMXBean;

import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;

public class VMUtils
{
  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

  public static boolean isThreadCpuTimeEnabled()
  {
    return THREAD_MX_BEAN.isThreadCpuTimeSupported() && THREAD_MX_BEAN.isThreadCpuTimeEnabled();
  }

  public static long safeGetThreadCpuTime()
  {
    if (!isThreadCpuTimeEnabled()) {
      return 0L;
    } else {
      return getCurrentThreadCpuTime();
    }
  }

  /**
   * Returns the total CPU time for current thread.
   * This method should be called after verifying that cpu time measurement for current thread is supported by JVM
   *
   * @return total CPU time for the current thread in nanoseconds.
   *
   * @throws java.lang.UnsupportedOperationException if the Java
   *                                                 virtual machine does not support CPU time measurement for
   *                                                 the current thread.
   */
  public static long getCurrentThreadCpuTime()
  {
    return THREAD_MX_BEAN.getCurrentThreadCpuTime();
  }

  public static long getMaxDirectMemory() throws UnsupportedOperationException
  {
    try {
      Class<?> vmClass = Class.forName("sun.misc.VM");
      Object maxDirectMemoryObj = vmClass.getMethod("maxDirectMemory").invoke(null);

      if (!(maxDirectMemoryObj instanceof Number)) {
        throw new UnsupportedOperationException(
            String.format("Cannot determine maxDirectMemory from [%s]", maxDirectMemoryObj)
        );
      } else {
        return ((Number) maxDirectMemoryObj).longValue();
      }
    }
    catch (ClassNotFoundException e) {
      throw new UnsupportedOperationException("No VM class, cannot do memory check.", e);
    }
    catch (NoSuchMethodException e) {
      throw new UnsupportedOperationException("VM.maxDirectMemory doesn't exist, cannot do memory check.", e);
    }
    catch (InvocationTargetException e) {
      throw new UnsupportedOperationException("static method shouldn't throw this", e);
    }
    catch (IllegalAccessException e) {
      throw new UnsupportedOperationException("public method, shouldn't throw this", e);
    }
  }

  public static void dumpHeap(String filePath, boolean live) throws IOException
  {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    HotSpotDiagnosticMXBean mxBean = ManagementFactory.newPlatformMXBeanProxy(
        server, "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
    mxBean.dumpHeap(filePath, live);
  }

  public static Object jcmd(String command, Object[] params) throws Exception
  {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    return server.invoke(
        new ObjectName("com.sun.management:type=DiagnosticCommand"),
        command, params, new String[]{String[].class.getName()}
    );
  }

  public static MBeanOperationInfo[] jcmdCommands() throws Exception
  {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    MBeanInfo mBeanInfo = server.getMBeanInfo(new ObjectName("com.sun.management:type=DiagnosticCommand"));
    return mBeanInfo.getOperations();
  }

  public static MBeanOperationInfo jcmdCommandHelp(String command) throws Exception
  {
    for (MBeanOperationInfo info : jcmdCommands()) {
      if (info.getName().equalsIgnoreCase(command)) {
        return info;
      }
    }
    return null;
  }
}
