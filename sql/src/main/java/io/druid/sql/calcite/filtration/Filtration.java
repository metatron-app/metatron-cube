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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import io.druid.data.TypeResolver;
import io.druid.java.util.common.ISE;
import io.druid.common.Intervals;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.sql.calcite.table.RowSignature;
import org.joda.time.Interval;

import java.util.List;

public class Filtration
{
  private static final DimFilter MATCH_NOTHING = new MathExprFilter("1 == 2");
  private static final DimFilter MATCH_EVERYTHING = new MathExprFilter("1 == 1");

  // 1) If "dimFilter" is null, it should be ignored and not affect filtration.
  // 2) There is an implicit AND between "intervals" and "dimFilter" (if dimFilter is non-null).
  // 3) There is an implicit OR between the intervals in "intervals".
  private final DimFilter dimFilter;
  private final List<Interval> intervals;

  private Filtration(final DimFilter dimFilter, final List<Interval> intervals)
  {
    this.intervals = intervals != null ? intervals : Intervals.ONLY_ETERNITY;
    this.dimFilter = dimFilter;
  }

  public static Interval eternity()
  {
    return Intervals.ETERNITY;
  }

  public static DimFilter matchNothing()
  {
    return MATCH_NOTHING;
  }

  public static DimFilter matchEverything()
  {
    return MATCH_EVERYTHING;
  }

  public static Filtration create(final DimFilter dimFilter)
  {
    return new Filtration(dimFilter, null);
  }

  public static Filtration create(final DimFilter dimFilter, final List<Interval> intervals)
  {
    return new Filtration(dimFilter, intervals);
  }

  private static Filtration transform(final Filtration filtration, final List<Function<Filtration, Filtration>> fns)
  {
    Filtration retVal = filtration;
    for (Function<Filtration, Filtration> fn : fns) {
      retVal = fn.apply(retVal);
    }
    return retVal;
  }

  public QuerySegmentSpec getQuerySegmentSpec()
  {
    return Intervals.ONLY_ETERNITY.equals(intervals) ? null : new MultipleIntervalSegmentSpec(intervals);
  }

  public List<Interval> getIntervals()
  {
    return intervals;
  }

  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  /**
   * Optimize a Filtration for querying, possibly pulling out intervals and simplifying the dimFilter in the process.
   *
   * @return equivalent Filtration
   */
  public Filtration optimize(final TypeResolver resolver)
  {
    return transform(
        this,
        ImmutableList.of(
            CombineAndSimplifyBounds.instance(),
            MoveTimeFiltersToIntervals.instance(),
            ConvertBoundsToSelectors.create(resolver),
            ConvertSelectorsToIns.create(resolver),
            MoveMarkerFiltersToIntervals.instance(),
            ValidateNoMarkerFiltersRemain.instance()
        )
    );
  }

  /**
   * Optimize a Filtration containing only a DimFilter, avoiding pulling out intervals.
   *
   * @return equivalent Filtration
   */
  public Filtration optimizeFilterOnly(final RowSignature sourceRowSignature)
  {
    if (!intervals.equals(ImmutableList.of(eternity()))) {
      throw new ISE("Cannot optimizeFilterOnly when intervals are set");
    }

    final Filtration transformed = transform(
        this,
        ImmutableList.of(
            CombineAndSimplifyBounds.instance(),
            ConvertBoundsToSelectors.create(sourceRowSignature),
            ConvertSelectorsToIns.create(sourceRowSignature)
        )
    );

    if (!transformed.getIntervals().equals(ImmutableList.of(eternity()))) {
      throw new ISE("Filtration [%s] has intervals [%s]", transformed.dimFilter, transformed.intervals);
    }

    return transformed;
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

    Filtration that = (Filtration) o;

    if (intervals != null ? !intervals.equals(that.intervals) : that.intervals != null) {
      return false;
    }
    return dimFilter != null ? dimFilter.equals(that.dimFilter) : that.dimFilter == null;

  }

  @Override
  public int hashCode()
  {
    int result = intervals != null ? intervals.hashCode() : 0;
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "Filtration{" +
           "intervals=" + intervals +
           ", dimFilter=" + dimFilter +
           '}';
  }
}
