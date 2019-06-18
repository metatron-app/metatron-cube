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

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

import java.util.Comparator;

public class DocumentsColumnAggregator implements Aggregator
{
  public static final Comparator COMPARATOR = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      DocumentsColumn dc1 = (DocumentsColumn)o1;
      DocumentsColumn dc2 = (DocumentsColumn)o2;

      if (dc1.numDoc == dc2.numDoc)
      {
        return dc1.get().get(0).compareTo(dc2.get().get(0));
      }

      return Integer.compare(dc1.numDoc, dc2.numDoc);
    }
  };

  public static DocumentsColumn combine(Object ldc, Object rdc)
  {
    return ((DocumentsColumn)ldc).add((DocumentsColumn)rdc);
  }

  private final ObjectColumnSelector selector;
  private final boolean compress;
  private DocumentsColumn documentsColumn;

  public DocumentsColumnAggregator(
      ObjectColumnSelector selector,
      boolean compress
  )
  {
    this.selector = selector;
    this.compress = compress;

    this.documentsColumn = new DocumentsColumn(compress);
  }

  @Override
  public void aggregate() {
    Object object = selector.get();
    if (object instanceof String) {
      documentsColumn.add((String)object);
    } else if (object instanceof DocumentsColumn) {
      documentsColumn.add((DocumentsColumn) object);
    }
  }

  @Override
  public void reset() {
    this.documentsColumn = new DocumentsColumn(compress);
  }

  @Override
  public Object get() {
    return documentsColumn;
  }

  @Override
  public Float getFloat() {
    throw new UnsupportedOperationException("DocumentsColumnAggregator does not support getFloat()");
  }

  @Override
  public void close() {
    if (documentsColumn != null)
    {
      documentsColumn = null;
    }
  }

  @Override
  public Long getLong() {
    throw new UnsupportedOperationException("DocumentsColumnAggregator does not support getLong()");
  }

  @Override
  public Double getDouble()
  {
    throw new UnsupportedOperationException("DocumentsColumnAggregator does not support getDouble()");
  }
}
