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

package io.druid.sql.calcite.filtration;

import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.SelectorDimFilter;

public class BoundRefKey
{
  private final String dimension;
  private final ExtractionFn extractionFn;
  private final String comparatorType;

  public BoundRefKey(String dimension, ExtractionFn extractionFn, String comparatorType)
  {
    this.dimension = dimension;
    this.extractionFn = extractionFn;
    this.comparatorType = comparatorType;
  }

  public static BoundRefKey from(BoundDimFilter filter)
  {
    return new BoundRefKey(
        filter.getDimension(),
        filter.getExtractionFn(),
        filter.getComparatorType()
    );
  }

  public static BoundRefKey from(SelectorDimFilter filter, String comparatorType)
  {
    return new BoundRefKey(
        filter.getDimension(),
        filter.getExtractionFn(),
        comparatorType
    );
  }

  public String getDimension()
  {
    return dimension;
  }

  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  public String getComparator()
  {
    return comparatorType;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BoundRefKey boundRefKey = (BoundRefKey) o;

    if (dimension != null ? !dimension.equals(boundRefKey.dimension) : boundRefKey.dimension != null) {
      return false;
    }
    if (extractionFn != null ? !extractionFn.equals(boundRefKey.extractionFn) : boundRefKey.extractionFn != null) {
      return false;
    }
    return comparatorType != null ? comparatorType.equals(boundRefKey.comparatorType) : boundRefKey.comparatorType == null;
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    result = 31 * result + (comparatorType != null ? comparatorType.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "BoundRefKey{" +
           "dimension='" + dimension + '\'' +
           ", extractionFn=" + extractionFn +
           ", comparator=" + comparatorType +
           '}';
  }
}
