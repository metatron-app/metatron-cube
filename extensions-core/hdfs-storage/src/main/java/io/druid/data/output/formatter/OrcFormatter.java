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

package io.druid.data.output.formatter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.data.output.Formatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

/**
 * to validate, use hive --orcfiledump -d path
 */
public class OrcFormatter implements Formatter
{
  private static final Logger log = new Logger(OrcFormatter.class);

  private final ObjectMapper mapper;
  private final List<String> columnNames;
  private final List<TypeDescription> columnTypes;

  private final Path path;
  private final FileSystem fs;
  private final Writer writer;
  private final VectorizedRowBatch batch;

  private int counter;

  public OrcFormatter(Path path, FileSystem fs, String typeString, ObjectMapper mapper) throws IOException
  {
    this.mapper = mapper;
    this.columnNames = Lists.newArrayList();
    StringBuilder builder = new StringBuilder();
    builder.append("struct<");
    for (String column : typeString.split(",")) {
      if (builder.length() > 7) {
        builder.append(",");
      }
      int index = column.indexOf(':');
      if (index < 0) {
        builder.append(column);
        builder.append(":string");
        columnNames.add(column);
      } else {
        String type = column.substring(index + 1, column.length());
        if (ValueDesc.isDimension(type)) {
          builder.append(column, 0, index);
          builder.append(':').append(ValueDesc.subElementOf(type));
        } else if (type.equalsIgnoreCase(ValueDesc.LONG_TYPE)) {
          builder.append(column, 0, index);
          builder.append(":bigint");
        } else {
          builder.append(column);
        }
        columnNames.add(column.substring(0, index));
      }
    }
    String schemaString = builder.append('>').toString();
    log.info("Applying schema : " + schemaString);

    ClassLoader prev = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(OrcFormatter.class.getClassLoader());
    try {
      Configuration conf = new Configuration();
      TypeDescription schema = TypeDescriptions.fromString(schemaString);
      columnTypes = schema.getChildren();
      writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).setSchema(schema));
      batch = schema.createRowBatch();
    }
    finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
    this.path = path;
    this.fs = fs;
  }

  @Override
  public void write(Map<String, Object> datum) throws IOException
  {
    final int rowId = batch.size++;
    for (int i = 0; i < columnNames.size(); i++) {
      Object object = datum.get(columnNames.get(i));
      setColumn(rowId, object, batch.cols[i], columnTypes.get(i));
    }
    if (batch.size == batch.getMaxSize()) {
      writer.addRowBatch(batch);
      batch.reset();
    }
    counter++;
  }

  private void setColumn(int rowId, Object object, ColumnVector vector, TypeDescription column)
      throws JsonProcessingException
  {
    if (object == null) {
      vector.isNull[rowId] = true;
      vector.noNulls = false;
      return;
    }
    switch (column.getCategory()) {
      case INT:
      case LONG:
        ((LongColumnVector) vector).vector[rowId] = Rows.parseLong(object);
        break;
      case FLOAT:
      case DOUBLE:
        ((DoubleColumnVector) vector).vector[rowId] = Rows.parseDouble(object);
        break;
      case STRING:
        ((BytesColumnVector) vector).setVal(rowId, StringUtils.toUtf8(object.toString()));
        break;
      case BINARY:
        byte[] b = object instanceof byte[] ? (byte[]) object : mapper.writeValueAsBytes(object);
        ((BytesColumnVector) vector).setVal(rowId, b, 1, b.length - 1);
        break;
      case LIST:
        final ListColumnVector list = (ListColumnVector) vector;
        final TypeDescription elementType = column.getChildren().get(0);
        final ColumnVector elements = list.child;
        final int offset = list.childCount;

        final int length;
        if (object instanceof List) {
          final List values = (List) object;
          length = values.size();
          elements.ensureSize(offset + length, true);
          for (int j = 0; j < length; j++) {
            setColumn(offset + j, values.get(j), elements, elementType);
          }
        } else if (object.getClass().isArray()) {
          length = Array.getLength(object);
          elements.ensureSize(offset + length, true);
          for (int j = 0; j < length; j++) {
            setColumn(offset + j, Array.get(object, j), elements, elementType);
          }
        } else {
          length = 1;
          elements.ensureSize(offset + length, true);
          setColumn(offset, object, elements, elementType);
        }
        list.offsets[rowId] = offset;
        list.lengths[rowId] = length;
        list.childCount += length;
        break;
      default:
        throw new UnsupportedOperationException("Not supported type " + column.getCategory());
    }
  }

  @Override
  public Map<String, Object> close() throws IOException
  {
    if (batch.size > 0) {
      writer.addRowBatch(batch);
    }
    writer.close();
    long length = fs.getFileStatus(path).getLen();
    return ImmutableMap.<String, Object>of(
        "rowCount", counter,
        "data", ImmutableMap.of(path.toString(), length)
    );
  }
}
