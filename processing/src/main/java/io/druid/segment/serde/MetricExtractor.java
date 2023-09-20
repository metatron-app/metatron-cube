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

import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.query.aggregation.ArrayMetricSerde;

import java.util.Arrays;
import java.util.List;

/**
 *
 */
public interface MetricExtractor
{
  MetricExtractor DUMMY = source -> source;

  default Object extractValue(Row inputRow, String metricName)
  {
    return extract(inputRow.getRaw(metricName));
  }

  Object extract(Object rawValue);

  default List<String> getExtractedNames(List<String> metricNames)
  {
    return Arrays.asList();
  }

  static MetricExtractor of(ValueDesc type)
  {
    if (type.isPrimitive()) {
      return type.type()::cast;
    }
    if (type.isStruct()) {
      return StructMetricSerde.parse(type).getExtractor(Arrays.asList());
    }
    if (type.isArray()) {
      return ArrayMetricSerde.extractor(type.unwrapArray());
    }
    MetricExtractor extractor = ComplexMetrics.getExtractor(type, Arrays.asList());
    return extractor == null ? DUMMY : extractor;
  }
}