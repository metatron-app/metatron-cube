/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.column;

import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedInts;

import java.io.Closeable;

/**
 */
public interface DictionaryEncodedColumn extends Closeable
{
  public int length();
  public boolean hasMultipleValues();
  public int getSingleValueRow(int rowNum);
  public IndexedInts getMultiValueRow(int rowNum);
  public String lookupName(int id);
  public int lookupId(String name);
  public int getCardinality();

  public GenericIndexed<String> dictionary();
}
