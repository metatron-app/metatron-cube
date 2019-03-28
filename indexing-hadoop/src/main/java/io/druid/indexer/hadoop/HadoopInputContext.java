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

package io.druid.indexer.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

public interface HadoopInputContext
{
  Configuration getConfiguration();

  InputSplit getInputSplit();

  <T> T unwrap(Class<T> clazz);

  class MapperContext implements HadoopInputContext
  {
    private final Mapper.Context context;

    public MapperContext(Mapper.Context context) {this.context = context;}

    @Override
    public Configuration getConfiguration()
    {
      return context.getConfiguration();
    }

    @Override
    public InputSplit getInputSplit()
    {
      return context.getInputSplit();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> clazz)
    {
      return clazz.isInstance(context) ? (T) context : null;
    }
  }
}
