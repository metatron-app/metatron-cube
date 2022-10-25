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

package io.druid.segment.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;

import java.io.IOException;

@JsonTypeName("fst.lucene7")
public class Lucene7FSTSerDe extends LuceneFSTSerDe
{
  @JsonCreator
  public Lucene7FSTSerDe(@JsonProperty("reduction") Float reduction)
  {
    super(reduction);
  }

  @Override
  protected byte[] save(FST<Long> make) throws IOException
  {
    return Lucenes.serialize(out -> make.save(out));
  }

  @Override
  protected FST<Long> load(DataInput input) throws IOException
  {
    return new FST<Long>(input, PositiveIntOutputs.getSingleton());
  }
}