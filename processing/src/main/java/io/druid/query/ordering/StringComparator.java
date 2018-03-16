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

package io.druid.query.ordering;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.common.Cacheable;

import java.util.Comparator;

/**
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type",
    defaultImpl = StringComparators.LexicographicComparator.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = StringComparators.LEXICOGRAPHIC_NAME, value = StringComparators.LexicographicComparator.class),
    @JsonSubTypes.Type(name = StringComparators.ALPHANUMERIC_NAME, value = StringComparators.AlphanumericComparator.class),
    @JsonSubTypes.Type(name = StringComparators.INTEGER_NAME, value = StringComparators.IntegerComparator.class),
    @JsonSubTypes.Type(name = StringComparators.LONG_NAME, value = StringComparators.LongComparator.class),
    @JsonSubTypes.Type(name = StringComparators.FLOATING_POINT_NAME, value = StringComparators.FloatingPointComparator.class),
    @JsonSubTypes.Type(name = StringComparators.NUMERIC_NAME, value = StringComparators.NumericComparator.class),
    @JsonSubTypes.Type(name = StringComparators.DAY_OF_WEEK_NAME, value = StringComparators.DayOfWeekComparator.class),
    @JsonSubTypes.Type(name = StringComparators.MONTH_NAME, value = StringComparators.MonthComparator.class)
})
public interface StringComparator extends Comparator<String>
{
}
