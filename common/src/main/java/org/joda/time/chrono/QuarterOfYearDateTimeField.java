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

package org.joda.time.chrono;

import org.joda.time.DurationField;
import org.joda.time.ReadablePartial;
import org.joda.time.field.FieldUtils;
import org.joda.time.field.ImpreciseDateTimeField;

/**
 */
public class QuarterOfYearDateTimeField extends ImpreciseDateTimeField
{
  private final int MIN = 1;
  private final int MAX = 4;

  private final int[] QUARTER_TO_MONTH = new int[]{-1, 1, 4, 7, 10};
  private final int[] MONTH_TO_QUARTER = new int[]{-1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4};
  private final int[] MONTH_ROUND = new int[]{-1, 1, 1, 1, 4, 4, 4, 7, 7, 7, 10, 10, 10};

  private final BasicChronology iChronology;

  public QuarterOfYearDateTimeField(BasicChronology chronology, QuarterFieldType type)
  {
    super(type, chronology.getAverageMillisPerMonth() * 3);
    this.iChronology = chronology;
  }

  @Override
  public boolean isLenient()
  {
    return false;
  }

  @Override
  public int get(long instant)
  {
    return MONTH_TO_QUARTER[iChronology.getMonthOfYear(instant)];
  }

  @Override
  public long add(long instant, int quarters)
  {
    throw new UnsupportedOperationException("add");
  }

  @Override
  public long add(long instant, long quarters)
  {
    throw new UnsupportedOperationException("add");
  }

  @Override
  public DurationField getRangeDurationField()
  {
    return iChronology.years();
  }

  @Override
  public int getMinimumValue()
  {
    return MIN;
  }

  @Override
  public int getMaximumValue()
  {
    return MAX;
  }

  @Override
  public long roundFloor(long instant)
  {
    int year = iChronology.getYear(instant);
    int month = MONTH_ROUND[iChronology.getMonthOfYear(instant, year)];
    return iChronology.getYearMonthMillis(year, month);
  }

  @Override
  public int[] add(ReadablePartial partial, int fieldIndex, int[] values, int valueToAdd)
  {
    throw new UnsupportedOperationException("add");
  }

  @Override
  public long set(long instant, int quarter)
  {
    FieldUtils.verifyValueBounds(this, quarter, MIN, MAX);
    int month = QUARTER_TO_MONTH[quarter];
    //
    int thisYear = iChronology.getYear(instant);
    //
    int thisDom = iChronology.getDayOfMonth(instant, thisYear);
    int maxDom = iChronology.getDaysInYearMonth(thisYear, month);
    if (thisDom > maxDom) {
      // Quietly force DOM to nearest sane value.
      thisDom = maxDom;
    }
    // Return newly calculated millis value
    return iChronology.getYearMonthDayMillis(thisYear, month, thisDom) +
           iChronology.getMillisOfDay(instant);
  }
}
