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

import io.druid.java.util.common.ISE;

import java.io.IOException;
import java.net.ServerSocket;

/**
 */
public class SocketUtil
{
  public static int findOpenPort(int startPort)
  {
    int currPort = startPort;

    while (currPort < 0xffff) {
      try (ServerSocket socket = new ServerSocket(currPort)) {
        return currPort;
      }
      catch (IOException e) {
        ++currPort;
      }
    }

    throw new ISE("Unable to find open port between[%d] and [%d]", startPort, currPort);
  }

  public static int findOpenPortFrom(int startPort)
  {
    int currPort = startPort;

    while (currPort < 0xffff) {
      try (ServerSocket socket = new ServerSocket(currPort)) {
        return currPort;
      }
      catch (IOException e) {
        ++currPort;
      }
    }

    throw new ISE("Unable to find open port between [%d] and [%d]", startPort, currPort);
  }
}
