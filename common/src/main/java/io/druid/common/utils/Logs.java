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

import com.google.common.base.Supplier;

import java.util.Objects;

public class Logs
{
  public static Object lazy(Supplier supplier)
  {
    return new Object()
    {
      @Override
      public String toString()
      {
        return Objects.toString(supplier.get(), null);
      }
    };
  }

  public static Object lazy(String format, Object x)
  {
    return lazy(() -> x == null ? "" : String.format(format, x));
  }
}
