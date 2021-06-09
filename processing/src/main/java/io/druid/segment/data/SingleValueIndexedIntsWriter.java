/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.segment.data;

import io.druid.java.util.common.IAE;

import java.io.IOException;

public abstract class SingleValueIndexedIntsWriter implements ColumnPartWriter
{
  @Override
  public void add(Object obj) throws IOException
  {
    if (obj == null) {
      add(0);
    } else if (obj instanceof Integer) {
      add(((Number) obj).intValue());
    } else if (obj instanceof int[]) {
      int[] vals = (int[]) obj;
      if (vals.length == 0) {
        add(0);
      } else {
        add(vals[0]);
      }
    } else {
      throw new IAE("Unsupported single value type: " + obj.getClass());
    }
  }

  public abstract void add(int val) throws IOException;
}
