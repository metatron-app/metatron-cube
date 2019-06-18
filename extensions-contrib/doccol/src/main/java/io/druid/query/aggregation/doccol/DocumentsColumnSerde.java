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

import com.google.common.collect.Ordering;
import io.druid.data.input.Row;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public class DocumentsColumnSerde extends ComplexMetricSerde
{
  private static Ordering<DocumentsColumn> comparator = new Ordering<DocumentsColumn>() {
    @Override
    public int compare(DocumentsColumn documentsColumn, DocumentsColumn t1) {
      return DocumentsColumnAggregator.COMPARATOR.compare(documentsColumn, t1);
    }
  }.nullsFirst();

  private ObjectStrategy strategy = new ObjectStrategy<DocumentsColumn>()
  {
    @Override
    public Class<? extends DocumentsColumn> getClazz()
    {
      return DocumentsColumn.class;
    }

    @Override
    public DocumentsColumn fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
      readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
      return DocumentsColumn.fromBytes(readOnlyBuffer);
    }

    @Override
    public byte[] toBytes(DocumentsColumn documentsColumn)
    {
      if (documentsColumn == null) {
        return new byte[]{};
      }
      return documentsColumn.toBytes();
    }

    @Override
    public int compare(DocumentsColumn dc1, DocumentsColumn dc2)
    {
      return comparator.compare(dc1, dc2);
    }
  };

  @Override
  public String getTypeName() {
    return "docColumn";
  }

  @Override
  public ComplexMetricExtractor getExtractor() {
    return new ComplexMetricExtractor() {
      @Override
      public Class<DocumentsColumn> extractedClass()
      {
        return DocumentsColumn.class;
      }

      @Override
      public DocumentsColumn extractValue(Row inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue instanceof DocumentsColumn) {
          return (DocumentsColumn) rawValue;
        } else {
          List<String> dimValues = inputRow.getDimension(metricName);
          if (dimValues != null && dimValues.size() > 0) {
            Iterator<String> values = dimValues.iterator();

            DocumentsColumn documentsColumn = new DocumentsColumn(true);

            while (values.hasNext()) {
              documentsColumn.add(values.next());
            }
            return documentsColumn;
          } else {
            return new DocumentsColumn(true);
          }
        }
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    final GenericIndexed column = GenericIndexed.read(buffer, getObjectStrategy());
    builder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy getObjectStrategy() {
    return strategy;
  }
}
