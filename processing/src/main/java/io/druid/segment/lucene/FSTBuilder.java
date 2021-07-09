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

import io.druid.java.util.common.logger.Logger;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;

public class FSTBuilder
{
  public static final Logger LOG = new Logger(FSTBuilder.class);

  private static final int CHECK_INTERVAL = 10000;

  private final float reduction;
  private final Builder<Long> _builder = new Builder<>(FST.INPUT_TYPE.BYTE4, PositiveIntOutputs.getSingleton());
  private final IntsRefBuilder _scratch = new IntsRefBuilder();

  private boolean disabled;

  public FSTBuilder()
  {
    this(-1);
  }

  public FSTBuilder(float reduction)
  {
    this.reduction = reduction;
  }

  public void addEntry(String key, long ix) throws IOException
  {
    if (disabled) {
      return;
    }
    if (ix > 0 && ix % CHECK_INTERVAL == 0 && reduction > 0 && _builder.getNodeCount() > ix * reduction) {
      disabled = true;
      return;
    }
    _builder.add(Util.toUTF16(key, _scratch), ix);
  }

  public FST<Long> done(int cardinality) throws IOException
  {
    return disabled || (reduction > 0 && _builder.getNodeCount() > cardinality * reduction) ? null : _builder.finish();
  }

  public FST<Long> done() throws IOException
  {
    return disabled ? null : _builder.finish();
  }

  public long getNodeCount()
  {
    return _builder.getNodeCount();
  }

  public long getArcCount()
  {
    return _builder.getArcCount();
  }
}
