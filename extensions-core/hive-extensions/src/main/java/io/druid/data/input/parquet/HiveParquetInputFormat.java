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

package io.druid.data.input.parquet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.segment.indexing.DataSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;

// uses parquet in hive, not like parquet-extension
public class HiveParquetInputFormat extends ParquetInputFormat
{
  public HiveParquetInputFormat()
  {
    super(ArrayToMap.class);
  }

  public static class ArrayToMap extends ReadSupport<Map<String, Object>>
  {
    private DataWritableReadSupport delegated;

    @Override
    public ReadSupport.ReadContext init(InitContext context)
    {
      MessageType readSchema = context.getFileSchema();
      Configuration configuration = context.getConfiguration();
      delegated = ReflectionUtils.newInstance(DataWritableReadSupport.class, configuration);

      String[] required = configuration.getStrings(DataSchema.REQUIRED_COLUMNS);
      if (required != null && required.length > 0) {
        Set<String> filter = Sets.newHashSet(required);
        List<Type> projected = Lists.newArrayList();
        for (Type field : readSchema.getFields()) {
          if (filter.contains(field.getName())) {
            projected.add(field);
          }
        }
        readSchema = new MessageType(readSchema.getName(), projected);
      }

      Map<String, String> contextMetadata = Maps.newHashMap();
      contextMetadata.put(DataWritableReadSupport.HIVE_TABLE_AS_PARQUET_SCHEMA, readSchema.toString());
      return new ReadContext(readSchema, contextMetadata);
    }

    @Override
    public RecordMaterializer<Map<String, Object>> prepareForRead(
        Configuration configuration, Map<String, String> map, MessageType messageType, ReadContext readContext
    )
    {
      final RecordMaterializer<ArrayWritable> materializer = delegated.prepareForRead(
          configuration, map, messageType, readContext
      );
      final Map<String, String> metadata = readContext.getReadSupportMetadata();
      final String schema = metadata.get(DataWritableReadSupport.HIVE_TABLE_AS_PARQUET_SCHEMA);
      final List<Type> fields = MessageTypeParser.parseMessageType(schema).getFields();
      return new RecordMaterializer<Map<String, Object>>()
      {
        @Override
        public Map<String, Object> getCurrentRecord()
        {
          return parseStruct(materializer.getCurrentRecord().get(), fields);
        }

        @Override
        public GroupConverter getRootConverter()
        {
          return materializer.getRootConverter();
        }
      };
    }
  }

  private static Map<String, Object> parseStruct(Writable[] values, List<Type> fields)
  {
    final Map<String, Object> event = Maps.newLinkedHashMap();
    for (int i = 0; i < values.length; i++) {
      Type field = fields.get(i);
      event.put(field.getName(), extract(field, values[i]));
    }
    return event;
  }

  private static Object extract(Type field, Writable source)
  {
    if (source == null) {
      return null;
    }
    final OriginalType originalType = field.getOriginalType();
    if (originalType != null) {
      switch (originalType) {
        case UTF8:
          return source.toString();
        case DATE:
          return ((DateWritable) source).get().getTime();
        case DECIMAL:
          final HiveDecimalWritable decimal = (HiveDecimalWritable) source;
          return new BigDecimal(new BigInteger(decimal.getInternalStorage()), decimal.getScale());
        case LIST:
          final ArrayWritable eArray = (ArrayWritable) source;
          final GroupType eType = field.asGroupType().getFields().get(0).asGroupType();
          final List<Object> list = Lists.newArrayList();
          for (Writable element : eArray.get()) {
            list.add(extract(eType.getType(0), element));
          }
          return list;
        case MAP:
          final ArrayWritable kvArray = (ArrayWritable) source;
          final GroupType kvType = field.asGroupType().getFields().get(0).asGroupType();
          final Map<Object, Object> map = Maps.newLinkedHashMap();
          for (Writable element : kvArray.get()) {
            Writable[] kv = ((ArrayWritable) element).get();
            map.put(extract(kvType.getType(0), kv[0]), extract(kvType.getType(1), kv[1]));
          }
          return map;
      }
    }
    if (field.isPrimitive()) {
      final PrimitiveType primitive = field.asPrimitiveType();
      switch (primitive.getPrimitiveTypeName()) {
        case INT64:
          return ((LongWritable) source).get();
        case INT32:
          return ((IntWritable) source).get();
        case BOOLEAN:
          return ((BooleanWritable) source).get();
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          return ((BytesWritable) source).getBytes();
        case FLOAT:
          return ((FloatWritable) source).get();
        case DOUBLE:
          return ((DoubleWritable) source).get();
        case INT96:
          return ((TimestampWritable) source).getTimestamp().getTime();
      }
    }
    return null;
  }
}
