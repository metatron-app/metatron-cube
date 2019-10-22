package io.druid.java.util.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

public final class Intervals
{
  public static final Interval ETERNITY = utc(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT);
  public static final ImmutableList<Interval> ONLY_ETERNITY = ImmutableList.of(ETERNITY);

  public static Interval utc(long startInstant, long endInstant)
  {
    return new Interval(startInstant, endInstant, ISOChronology.getInstanceUTC());
  }

  public static Interval of(String interval)
  {
    return new Interval(interval, ISOChronology.getInstanceUTC());
  }

  public static Interval of(DateTime start, DateTime end)
  {
    Preconditions.checkArgument(DateTimeUtils.getInstantChronology(start) == DateTimeUtils.getInstantChronology(end));
    return new Interval(start, end);
  }

  private Intervals()
  {
  }
}
