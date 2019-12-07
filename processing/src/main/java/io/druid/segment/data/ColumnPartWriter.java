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

import io.druid.segment.serde.ColumnPartSerde;

import java.io.Closeable;
import java.io.IOException;

public interface ColumnPartWriter<T> extends ColumnPartSerde.Serializer, Closeable
{
  void open() throws IOException;

  void add(T obj) throws IOException;

  void close() throws IOException;

  abstract class Abstract<T> extends ColumnPartSerde.Serializer.Abstract implements ColumnPartWriter<T>
  {
    @Override
    public void close() throws IOException {}
  }
}
