/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.query.aggregation.doccol;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class DocumentsColumnBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector selector;
  private final boolean compress;
  private Map<Integer, DocumentsColumn> docMap;

  public DocumentsColumnBufferAggregator(
      ObjectColumnSelector selector,
      boolean compress
  )
  {
    this.selector = selector;
    this.compress = compress;
    this.docMap = new HashMap<>();
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.putInt(0);
    mutationBuffer.put(compress ? DocumentsColumn.COMPRESS_FLAG : 0);
    docMap.put(position, new DocumentsColumn(compress));
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    DocumentsColumn documentsColumn = docMap.get(position);
    Object object = selector.get();
    if (object instanceof String) {
      documentsColumn.add((String) object);
    } else if (object instanceof DocumentsColumn) {
      documentsColumn.add((DocumentsColumn) object);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position) {
    return docMap.get(position);
  }

  @Override
  public void close()
  {
    docMap.clear();
  }
}
