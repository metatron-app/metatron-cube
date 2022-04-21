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
import io.druid.java.util.common.logger.Logger;
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
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

// uses parquet in hive, not like one in parquet-extension (DruidParquetInputFormat)
public class HiveParquetInputFormat extends ParquetInputFormat
{
  public static final String COLUMNS_AS_LOWERCASE = "parquet.columns.as.lowercase";

  private static final Logger LOG = new Logger(HiveParquetInputFormat.class);

  @SuppressWarnings("unchecked")
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

      boolean lowercase = configuration.getBoolean(COLUMNS_AS_LOWERCASE, false);
      String[] required = configuration.getStrings(DataSchema.REQUIRED_COLUMNS);
      if (required != null && required.length > 0) {
        Set<String> filter = Sets.newHashSet(required);
        List<Type> projected = Lists.newArrayList();
        for (Type field : readSchema.getFields()) {
          if (filter.contains(asLowercase(field.getName(), lowercase))) {
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
      final boolean lowercase = configuration.getBoolean(COLUMNS_AS_LOWERCASE, false);
      final Map<String, String> metadata = readContext.getReadSupportMetadata();
      final String schema = metadata.get(DataWritableReadSupport.HIVE_TABLE_AS_PARQUET_SCHEMA);
      final List<Type> fields = MessageTypeParser.parseMessageType(schema).getFields();
      LOG.info("Reading parquet fields : %s", fields);

      final List<String> fieldNames = Lists.newArrayList();
      final List<Function<Writable, Object>> extractors = Lists.newArrayList();
      for (Type field : fields) {
        fieldNames.add(asLowercase(field.getName(), lowercase));
        extractors.add(extract(field));
      }

      return new RecordMaterializer<Map<String, Object>>()
      {
        @Override
        public Map<String, Object> getCurrentRecord()
        {
          return parseStruct(materializer.getCurrentRecord().get(), fieldNames, extractors);
        }

        @Override
        public GroupConverter getRootConverter()
        {
          return materializer.getRootConverter();
        }
      };
    }
  }

  private static String asLowercase(String value, boolean lowercase)
  {
    return lowercase ? value.toLowerCase() : value;
  }

  private static Map<String, Object> parseStruct(
      final Writable[] values,
      final List<String> fieldNames,
      final List<Function<Writable, Object>> extractors
  )
  {
    final Map<String, Object> event = Maps.newLinkedHashMap();
    for (int i = 0; i < values.length; i++) {
      event.put(fieldNames.get(i), extractors.get(i).apply(values[i]));
    }
    return event;
  }

  private static Function<Writable, Object> extract(Type field)
  {
    final OriginalType originalType = field.getOriginalType();
    if (originalType != null) {
      switch (originalType) {
        case UTF8:
          return w -> Objects.toString(w, null);
        case DATE:
          return w -> w == null ? null : ((DateWritable) w).get().getTime();
        case DECIMAL:
          return w -> {
            if (w == null) {
              return null;
            }
            final HiveDecimalWritable decimal = (HiveDecimalWritable) w;
            return new BigDecimal(new BigInteger(decimal.getInternalStorage()), decimal.getScale());
          };
        case LIST:
          final GroupType eType = field.asGroupType().getFields().get(0).asGroupType();
          final Function<Writable, Object> e = extract(eType.getType(0));
          return w -> {
            if (w == null) {
              return null;
            }
            final Writable[] eArray = ((ArrayWritable) w).get();
            final List<Object> list = Lists.newArrayListWithExpectedSize(eArray.length);
            for (Writable element : eArray) {
              list.add(e.apply(element));
            }
            return list;
          };
        case MAP:
          final GroupType kvType = field.asGroupType().getFields().get(0).asGroupType();
          final Function<Writable, Object> ke = extract(kvType.getType(0));
          final Function<Writable, Object> ve = extract(kvType.getType(1));
          return w -> {
            if (w == null) {
              return null;
            }
            final Writable[] kvArray = ((ArrayWritable) w).get();
            final Map<Object, Object> map = Maps.newLinkedHashMap();
            for (Writable element : kvArray) {
              final Writable[] kv = ((ArrayWritable) element).get();
              map.put(ke.apply(kv[0]), ve.apply(kv[1]));
            }
            return map;
          };
      }
    }
    if (field.isPrimitive()) {
      final PrimitiveType primitive = field.asPrimitiveType();
      switch (primitive.getPrimitiveTypeName()) {
        case INT64:
          return w -> w == null ? null : ((LongWritable) w).get();
        case INT32:
          return w -> w == null ? null : ((IntWritable) w).get();
        case BOOLEAN:
          return w -> w == null ? null : ((BooleanWritable) w).get();
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          return w -> w == null ? null : ((BytesWritable) w).getBytes();
        case FLOAT:
          return w -> w == null ? null : ((FloatWritable) w).get();
        case DOUBLE:
          return w -> w == null ? null : ((DoubleWritable) w).get();
        case INT96:
          return w -> w == null ? null : ((TimestampWritable) w).getTimestamp().getTime();
      }
    }
    LOG.warn("Not supported type [%s], ignoring..", field.toString());
    return w -> null;
  }
}
