/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

import java.util.StringTokenizer;

public class JvmUtils
{
  private static final boolean IS_JAVA9_COMPATIBLE = isJava9Compatible(System.getProperty("java.specification.version"));

  private static boolean isJava9Compatible(String versionString)
  {
    final StringTokenizer st = new StringTokenizer(versionString, ".");
    int majorVersion = Integer.parseInt(st.nextToken());

    return majorVersion >= 9;
  }

  public static boolean isIsJava9Compatible()
  {
    return IS_JAVA9_COMPATIBLE;
  }
}
