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

package io.druid.segment.serde;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.ArrayMetricSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;

/**
 */
public class ComplexMetrics
{
  private static final Logger log = new Logger(ComplexMetrics.class);
  private static final Map<String, ComplexMetricSerde.Factory> complexSerializerFactories = Maps.newHashMap();
  private static final Map<String, ComplexMetricSerde> complexSerializers = Maps.newHashMap();
  private static final Map<Class, String> classToTypeName = Maps.newHashMap();

  public static Comparator getComparator(ValueDesc type)
  {
    if (type.isPrimitive()) {
      return type.comparator();
    } else {
      final ComplexMetricSerde serde = getSerdeForType(type.typeName());
      final ObjectStrategy strategy = serde == null ? null : serde.getObjectStrategy();
      return strategy instanceof Comparator ? (Comparator) strategy : null;
    }
  }

  public static ComplexMetricSerde getSerdeForType(ValueDesc type)
  {
    return getSerdeForType(type.typeName());
  }

  public static ComplexMetricSerde getSerdeForType(String type)
  {
    ComplexMetricSerde serde = complexSerializers.get(type);
    if (serde == null) {
      String[] descriptiveType = TypeUtils.splitDescriptiveType(type);
      if (descriptiveType != null) {
        ComplexMetricSerde.Factory factory = complexSerializerFactories.get(descriptiveType[0]);
        if (factory != null) {
          serde = factory.create(descriptiveType);
        }
      }
    }
    return serde;
  }

  public static ComplexMetricSerde.Factory getSerdeFactory(String type)
  {
    return complexSerializerFactories.get(type);
  }

  public static String getTypeNameForClass(Class clazz)
  {
    return classToTypeName.get(clazz);
  }

  public static void registerSerdeFactory(String type, ComplexMetricSerde.Factory factory)
  {
    if (complexSerializerFactories.containsKey(type)) {
      throw new ISE("Serde for type [%s] already exists.", type);
    }
    complexSerializerFactories.put(type, factory);
  }

  public static void registerSerde(String type, ComplexMetricSerde serde)
  {
    if (complexSerializers.containsKey(type)) {
      throw new ISE("Serde for type [%s] already exists.", type);
    }
    addToMap(type, serde);
    if (!ValueDesc.isArray(type)) {
      ValueDesc arrayType = ValueDesc.ofArray(type);
      if (!complexSerializers.containsKey(arrayType.typeName())) {
        registerSerde(arrayType.typeName(), new ArrayMetricSerde(serde));
      }
      log.info("Serde for type [%s] is registered with class [%s]", type, serde.getClass());
    }
  }

  private static synchronized void addToMap(String type, ComplexMetricSerde serde)
  {
    complexSerializers.put(type, serde);
    classToTypeName.put(serde.getObjectStrategy().getClazz(), type);
  }

  static Supplier<ImmutableBitmap> readBitmap(ByteBuffer buffer, final BitmapSerdeFactory serdeFactory)
  {
    if (buffer.remaining() > Integer.BYTES) {
      final int size = buffer.getInt();
      final ByteBuffer serialized = ByteBufferSerializer.prepareForRead(buffer, size);
      return new Supplier<ImmutableBitmap>()
      {
        @Override
        public ImmutableBitmap get()
        {
          return serdeFactory.getObjectStrategy().fromByteBuffer(serialized, size);
        }
      };
    }
    return Suppliers.<ImmutableBitmap>ofInstance(serdeFactory.getBitmapFactory().makeEmptyImmutableBitmap());
  }
}
