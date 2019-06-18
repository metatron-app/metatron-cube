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

import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DurationFieldType;

/**
 */
public class WeekInMonthFieldType extends DateTimeFieldType
{
  public WeekInMonthFieldType()
  {
    super("weekOfMonth");
  }

  @Override
  public DurationFieldType getDurationType()
  {
    return new QuarterDurationFieldType();
  }

  @Override
  public DurationFieldType getRangeDurationType()
  {
    throw new UnsupportedOperationException("getRangeDurationType");
  }

  @Override
  public DateTimeField getField(Chronology chronology)
  {
    return new WeekInMonthDateTimeField(findBasicChronology(chronology), this);
  }

  private BasicChronology findBasicChronology(Chronology chronology)
  {
    if (chronology instanceof BasicChronology) {
      return (BasicChronology) chronology;
    }
    if (chronology instanceof AssembledChronology) {
      return findBasicChronology(((AssembledChronology) chronology).getBase());
    }
    throw new IllegalArgumentException("cannot find basic chronology in " + chronology);
  }

  @Override
  public boolean equals(Object o)
  {
    return this == o || getName().equals(((DateTimeFieldType) o).getName());
  }

  @Override
  public int hashCode()
  {
    return getName().hashCode();
  }
}