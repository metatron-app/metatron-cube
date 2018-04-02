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

package io.druid.segment;

import org.joda.time.Interval;

import java.io.IOException;

/**
 */
public class StorageAdapterSegment extends AbstractSegment
{
  private final String identifier;
  private final StorageAdapter adapter;

  public StorageAdapterSegment(String identifier, StorageAdapter adapter) {
    this.identifier = identifier;
    this.adapter = adapter;
  }

  @Override
  public String getIdentifier()
  {
    return identifier;
  }

  @Override
  public Interval getDataInterval()
  {
    return adapter.getInterval();
  }

  @Override
  public QueryableIndex asQueryableIndex(boolean forQuery)
  {
    return null;
  }

  @Override
  public StorageAdapter asStorageAdapter(boolean forQuery)
  {
    accessed(forQuery);
    return adapter;
  }

  @Override
  public void close() throws IOException
  {
  }
}
