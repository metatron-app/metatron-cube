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

package io.druid.segment;

import io.druid.data.ValueDesc;
import io.druid.segment.column.ColumnDescriptor.Builder;
import io.druid.segment.data.IOPeon;

import java.io.Closeable;
import java.io.IOException;

/**
 * open -> serialize -> close -> build -> serde.getSerializedSize -> serde.writeToChannel
 */
public interface MetricColumnSerializer extends Closeable
{
  MetricColumnSerializer DUMMY = new MetricColumnSerializer()
  {
    @Override
    public void open(IOPeon ioPeon) throws IOException {}

    @Override
    public void serialize(int rowNum, Object aggs) throws IOException {}

    @Override
    public Builder buildDescriptor(Builder builder) throws IOException {return builder;}
  };

  void open(IOPeon ioPeon) throws IOException;

  void serialize(int rowNum, Object aggs) throws IOException;

  @Override
  default void close() throws IOException {}

  Builder buildDescriptor(Builder builder) throws IOException;

  // for deprecated classes
  abstract class Deprecated implements MetricColumnSerializer
  {
    @Override
    public Builder buildDescriptor(Builder builder) throws IOException
    {
      throw new UnsupportedOperationException("buildDescriptor");
    }
  }

  interface Factory
  {
    MetricColumnSerializer create(String name, ValueDesc type) throws IOException;
  }
}
