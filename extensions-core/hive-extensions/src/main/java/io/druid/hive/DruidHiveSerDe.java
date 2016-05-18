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

package io.druid.hive;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import io.druid.indexer.hadoop.MapWritable;
import io.druid.indexer.hadoop.QueryBasedInputFormat;
import io.druid.query.select.EventHolder;
import io.druid.segment.column.Column;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 */
public class DruidHiveSerDe extends AbstractSerDe
{
  private List<Converter> converters;
  private ObjectInspector inspector;

  @Override
  public void initialize(Configuration configuration, Properties properties) throws SerDeException
  {
    LazySerDeParameters serdeParams = new LazySerDeParameters(configuration, properties, getClass().getName());

    List<String> columnNames = serdeParams.getColumnNames();
    List<TypeInfo> columnTypes = serdeParams.getColumnTypes();

    String timeColumn = properties.getProperty(
        QueryBasedInputFormat.CONF_DRUID_TIME_COLUMN_NAME,
        Column.TIME_COLUMN_NAME
    );

    converters = Lists.newArrayListWithExpectedSize(columnNames.size());
    for (int i = 0; i < columnTypes.size(); ++i) {
      String columnName = columnNames.get(i);
      TypeInfo columnType = columnTypes.get(i);
      converters.add(toConverter(columnName, columnType, columnName.equals(timeColumn)));
    }

    inspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        columnNames, Lists.transform(
            converters, new Function<Converter, ObjectInspector>()
            {
              @Override
              public ObjectInspector apply(Converter input)
              {
                return input.inspector;
              }
            }
        )
    );
  }

  private Converter toConverter(String columnName, TypeInfo columnType, boolean timeColumn)
  {
    switch (columnType.getCategory()) {
      case PRIMITIVE:
        PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) columnType;
        Function<Object, Object> converter = ExpressionConverter.converter(ptype);
        if (timeColumn) {
          converter = Functions.compose(converter, TIME_EXTRACTOR);
          columnName = EventHolder.timestampKey;
        }
        ObjectInspector objectInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(ptype);
        return new Converter(columnName, objectInspector, converter);
      case LIST:
        TypeInfo element = ((ListTypeInfo) columnType).getListElementTypeInfo();
        if (element.getCategory() != ObjectInspector.Category.PRIMITIVE) {
          throw new UnsupportedOperationException("Not supported element type " + element);
        }
        PrimitiveTypeInfo elementType = (PrimitiveTypeInfo) element;
        ObjectInspector listObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(elementType)
        );
        return new Converter(columnName, listObjectInspector, ExpressionConverter.listConverter());
      default:
        // todo
        throw new UnsupportedOperationException("Not supported element type " + columnType);
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public SerDeStats getSerDeStats()
  {
    return new SerDeStats();
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException
  {
    Map<String, Object> value = ((MapWritable) writable).getValue();
    List output = Lists.newArrayListWithExpectedSize(converters.size());
    for (Converter c : converters) {
      output.add(c.apply(value));
    }
    return output;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException
  {
    return inspector;
  }

  private static final Function<Object, Object> TIME_EXTRACTOR = new Function<Object, Object>()
  {
    @Override
    public Object apply(Object input)
    {
      return input instanceof DateTime ? ((DateTime)input).getMillis() : new DateTime(input).getMillis();
    }
  };

  private class Converter
  {
    private final String column;
    private final Function<Object, Object> converter;
    private final ObjectInspector inspector;

    private Converter(
        String column,
        ObjectInspector inspector,
        Function<Object, Object> converter
    )
    {
      this.column = column;
      this.converter = converter;
      this.inspector = inspector;
    }

    private Object apply(Map<String, Object> input)
    {
      return converter.apply(input.get(column));
    }
  }
}
