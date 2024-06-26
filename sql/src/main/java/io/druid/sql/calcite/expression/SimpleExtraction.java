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

package io.druid.sql.calcite.expression;

import com.google.common.base.Preconditions;
import io.druid.common.Cacheable;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.ExtractionFn;

/**
 * Represents a "simple" extraction of a value from a Druid row, which is defined as a column plus an extractionFn.
 * This is useful since identifying simple extractions and treating them specially can allow Druid to perform
 * additional optimizations.
 */
public class SimpleExtraction implements Cacheable
{
  public static SimpleExtraction of(String column, ExtractionFn extractionFn)
  {
    return new SimpleExtraction(column, extractionFn);
  }

  private final String column;
  private final ExtractionFn extractionFn;

  private SimpleExtraction(String column, ExtractionFn extractionFn)
  {
    this.column = Preconditions.checkNotNull(column, "column");
    this.extractionFn = extractionFn;
  }

  public String getColumn()
  {
    return column;
  }

  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  public SimpleExtraction cascade(final ExtractionFn nextExtractionFn)
  {
    return new SimpleExtraction(
        column,
        ExtractionFns.cascade(extractionFn, Preconditions.checkNotNull(nextExtractionFn, "nextExtractionFn"))
    );
  }

  public DimensionSpec toDimensionSpec(String outputName)
  {
    return extractionFn == null
           ? new DefaultDimensionSpec(column, outputName)
           : new ExtractionDimensionSpec(column, outputName, extractionFn);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(column).append(extractionFn);
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

    SimpleExtraction that = (SimpleExtraction) o;

    if (column != null ? !column.equals(that.column) : that.column != null) {
      return false;
    }
    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;

  }

  @Override
  public int hashCode()
  {
    int result = column != null ? column.hashCode() : 0;
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    if (extractionFn != null) {
      return StringUtils.format("%s(%s)", extractionFn, column);
    } else {
      return column;
    }
  }
}
