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

package io.druid.segment.column;

import io.druid.data.ValueType;
import io.druid.segment.data.GenericIndexed;

/**
 */
public class IndexedStringsGenericColumn extends AbstractGenericColumn
{
  private final GenericIndexed<String> indexed;

  public IndexedStringsGenericColumn(GenericIndexed<String> indexed)
  {
    this.indexed = indexed;
  }

  @Override
  public int length()
  {
    return indexed.size();
  }

  @Override
  public ValueType getType()
  {
    return ValueType.STRING;
  }

  @Override
  public String getString(int rowNum)
  {
    return indexed.get(rowNum);
  }

  @Override
  public Object getValue(int rowNum)
  {
    return getString(rowNum);
  }
}
