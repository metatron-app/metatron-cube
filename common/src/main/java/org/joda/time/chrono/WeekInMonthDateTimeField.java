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

package org.joda.time.chrono;

import org.joda.time.DateTimeConstants;
import org.joda.time.DurationField;
import org.joda.time.ReadablePartial;
import org.joda.time.field.ImpreciseDateTimeField;

/**
 */
public class WeekInMonthDateTimeField extends ImpreciseDateTimeField
{
  private static final int MIN = 0;
  private static final int MAX = 5;

  private final BasicChronology iChronology;

  public WeekInMonthDateTimeField(BasicChronology chronology, WeekInMonthFieldType type)
  {
    super(type, DateTimeConstants.MILLIS_PER_WEEK);
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
    // copied from ICU
    int dayOfMonth = iChronology.getDayOfMonth(instant);
    int dayOfWeek = iChronology.getDayOfWeek(instant);
    int firstDayOfWeek = iChronology.dayOfWeek().getMinimumValue(instant);
    // Determine the day of the week of the first day of the period
    // in question (either a year or a month).  Zero represents the
    // first day of the week on this calendar.
    int periodStartDayOfWeek = (dayOfWeek - firstDayOfWeek - dayOfMonth + 1) % 7;
    if (periodStartDayOfWeek < 0) periodStartDayOfWeek += 7;

    // Compute the week number.  Initially, ignore the first week, which
    // may be fractional (or may not be).  We add periodStartDayOfWeek in
    // order to fill out the first week, if it is fractional.
    int weekNo = (dayOfMonth + periodStartDayOfWeek - 1)/7;

    // If the first week is long enough, then count it.  If
    // the minimal days in the first week is one, or if the period start
    // is zero, we always increment weekNo.
    if ((7 - periodStartDayOfWeek) >= iChronology.getMinimumDaysInFirstWeek()) ++weekNo;

    return weekNo;
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
    return iChronology.months();
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
    // todo
    throw new UnsupportedOperationException("roundFloor");
  }

  @Override
  public int[] add(ReadablePartial partial, int fieldIndex, int[] values, int valueToAdd)
  {
    throw new UnsupportedOperationException("add");
  }

  @Override
  public long set(long instant, int value)
  {
    // todo
    throw new UnsupportedOperationException("set");
  }
}
