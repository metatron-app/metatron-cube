/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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
package io.druid.data.input.orc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.TimestampSpec;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.indexer.hadoop.HadoopAwareParser;
import io.druid.indexer.path.HynixCombineInputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class OrcHadoopInputRowParser implements HadoopAwareParser<OrcStruct>
{
  private static final Logger logger = new Logger(OrcHadoopInputRowParser.class);

  private final ParseSpec parseSpec;
  private final String typeString;
  private final List<String> dimensions;
  private final TimestampSpec timestampSpec;

  private Context context;
  private Path currentPath;
  private StructObjectInspector dynamicInspector;

  @JsonCreator
  public OrcHadoopInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("typeString") String typeString,
      @JsonProperty("schema") String schema
  )
  {
    this.parseSpec = parseSpec;
    this.typeString = getTypeString(typeString, schema);
    this.dimensions = parseSpec.getDimensionsSpec().getDimensionNames();
    this.timestampSpec = parseSpec.getTimestampSpec();
  }

  private String getTypeString(String typeString, String schema)
  {
    return typeString != null ? typeString : schema != null ? typeStringFromSchema(schema) : null;
  }

  @Override
  public void setup(Context context) throws IOException
  {
    this.context = context;
  }

  @Override
  public InputRow parse(OrcStruct input)
  {
    StructObjectInspector oip = Preconditions.checkNotNull(
        getObjectInspector(),
        "user should specify typeString or schema"
    );

    List<? extends StructField> fields = oip.getAllStructFieldRefs();
    final Map<String, Object> map = Maps.newHashMapWithExpectedSize(fields.size());
    for (StructField field : fields) {
      ObjectInspector objectInspector = field.getFieldObjectInspector();
      switch (objectInspector.getCategory()) {
        case PRIMITIVE:
          PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) objectInspector;
          map.put(
              field.getFieldName(),
              getDatum(primitiveObjectInspector, oip.getStructFieldData(input, field))
          );
          break;
        case LIST:  // array case - only 1-depth array supported yet
          ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
          map.put(
              field.getFieldName(),
              getListObject(listObjectInspector, oip.getStructFieldData(input, field))
          );
          break;
        default:
          break;
      }
    }

    DateTime timestamp = timestampSpec.extractTimestamp(map);
    return new MapBasedInputRow(timestamp, dimensions, map);
  }

  private Object getDatum(PrimitiveObjectInspector inspector, Object field)
  {
    Object datum = inspector.getPrimitiveJavaObject(field);
    if (datum == null) {
      return null;
    }
    final PrimitiveObjectInspector.PrimitiveCategory category = inspector.getPrimitiveCategory();
    if (category == PrimitiveObjectInspector.PrimitiveCategory.CHAR) {
      datum = ((HiveChar) datum).getValue();
    } else if (category == PrimitiveObjectInspector.PrimitiveCategory.VARCHAR) {
      datum = ((HiveVarchar) datum).getValue();
    } else if (category == PrimitiveObjectInspector.PrimitiveCategory.DECIMAL) {
      datum = ((HiveDecimal) datum).bigDecimalValue();
    } else if (category == PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP) {
      datum = ((Timestamp) datum).getTime();
    }
    return datum;
  }

  private StructObjectInspector getObjectInspector()
  {
    if (typeString != null) {
      return initStaticInspector(typeString);
    }
    InputSplit split = context.getInputSplit();
    Path path;
    if (split instanceof HynixCombineInputFormat.HynixSplit) {
      path = HynixCombineInputFormat.CURRENT_PATH.get();
    } else if (split instanceof FileSplit) {
      path = ((FileSplit)split).getPath();
    } else {
      throw new IllegalArgumentException("Cannot access path in split " + split);
    }
    if (currentPath == null || !Objects.equals(currentPath, path)) {
      try {
        Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(context.getConfiguration()));
        dynamicInspector = (StructObjectInspector) reader.getObjectInspector();
        logger.info("Using ObjectInspector in orc meta %s", dynamicInspector.toString());
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
      currentPath = path;
    }
    return dynamicInspector;
  }

  // optional type string
  private StructObjectInspector initStaticInspector(String typeString)
  {
    logger.info("Using user specified spec %s", typeString);
    OrcSerde serde = new OrcSerde();
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeString);
    Properties table = getTablePropertiesFromStructTypeInfo((StructTypeInfo) typeInfo);
    serde.initialize(new Configuration(), table);
    try {
      return (StructObjectInspector) serde.getObjectInspector();
    }
    catch (SerDeException e) {
      throw Throwables.propagate(e);
    }
  }

  private List getListObject(ListObjectInspector listObjectInspector, Object listObject)
  {
    List objectList = listObjectInspector.getList(listObject);
    List list = null;
    ObjectInspector child = listObjectInspector.getListElementObjectInspector();
    switch(child.getCategory())
    {
      case PRIMITIVE:
        final PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector)child;
        list = Lists.transform(objectList, new Function() {
          @Nullable
          @Override
          public Object apply(@Nullable Object input) {
            return getDatum(primitiveObjectInspector, input);
          }
        });
        break;
      default:
        break;
    }

    return list;
  }

  @Override
  @JsonProperty
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @JsonProperty
  public String getTypeString()
  {
    return typeString;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new OrcHadoopInputRowParser(parseSpec, typeString, null);
  }

  public InputRowParser withTypeString(String typeString)
  {
    return new OrcHadoopInputRowParser(parseSpec, typeString, null);
  }

  private String typeStringFromSchema(String schema)
  {
    StringBuilder builder = new StringBuilder("struct<");
    for (String element : schema.split(",")) {
      if (builder.length() > 7) {
        builder.append(',');
      }
      builder.append(element);
      if (!element.contains(":")) {
        builder.append(":string");
      }
    }
    builder.append('>');

    return builder.toString();
  }

  public static Properties getTablePropertiesFromStructTypeInfo(StructTypeInfo structTypeInfo)
  {
    Properties table = new Properties();
    table.setProperty("columns", StringUtils.join(structTypeInfo.getAllStructFieldNames(), ","));
    table.setProperty("columns.types", StringUtils.join(
        Lists.transform(structTypeInfo.getAllStructFieldTypeInfos(),
            new Function<TypeInfo, String>() {
              @Override
              public String apply(TypeInfo typeInfo) {
                return typeInfo.getTypeName();
              }
            }),
        ","
    ));

    return table;
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length != 2) {
      throw new IllegalArgumentException(Arrays.toString(args));
    }
    Path path = new Path(args[0]);
    Configuration conf = new Configuration();
    Reader r = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    int index = args[1].indexOf('=');
    TimestampSpec timeSpec = new DefaultTimestampSpec(
        args[1].substring(0, index).trim(),
        args[1].substring(index).trim(),
        null
    );
    StructTypeInfo typeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(
        r.getObjectInspector().getTypeName()
    );

    List<String> names = typeInfo.getAllStructFieldNames();
    List<DimensionSchema> dimensions = Lists.transform(
        names, new Function<String, DimensionSchema>()
        {
          @Override
          public DimensionSchema apply(String input)
          {
            return new StringDimensionSchema(input);
          }
        }
    );
    ParseSpec spec = new TimeAndDimsParseSpec(timeSpec, new DimensionsSpec(dimensions, null, null));

    OrcHadoopInputRowParser parser = new OrcHadoopInputRowParser(spec, args[2], null);

    FileStatus status = path.getFileSystem(conf).getFileStatus(path);
    FileSplit split = new FileSplit(path, 0, status.getLen(), null);

    InputFormat inputFormat = ReflectionUtils.newInstance(OrcNewInputFormat.class, conf);

    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

    try (RecordReader reader = inputFormat.createRecordReader(split, context)) {
      while (reader.nextKeyValue()) {
        OrcStruct data = (OrcStruct) reader.getCurrentValue();
        MapBasedInputRow row = (MapBasedInputRow) parser.parse(data);
        StringBuilder builder = new StringBuilder();
        for (String dim : row.getDimensions()) {
          if (builder.length() > 0) {
            builder.append(", ");
          }
          builder.append(row.getRaw(dim));
        }
        System.out.println(builder.toString());
      }
    }
  }
}
