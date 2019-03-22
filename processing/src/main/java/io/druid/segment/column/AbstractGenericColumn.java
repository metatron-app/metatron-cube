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

import com.metamx.collections.bitmap.ImmutableBitmap;

import java.io.IOException;

/**
 */
public abstract class AbstractGenericColumn implements GenericColumn
{
  @Override
  public String getString(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Float getFloat(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Long getLong(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Double getDouble(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ImmutableBitmap getNulls()
  {
    return null;
  }

  @Override
  public void close() throws IOException {}
}
