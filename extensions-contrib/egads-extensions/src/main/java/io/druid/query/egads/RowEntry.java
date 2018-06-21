package io.druid.query.egads;

import com.yahoo.egads.data.TimeSeries;

/**
 */
public class RowEntry<T> extends TimeSeries.Entry
{
  final T row;

  public RowEntry(long timestamp, float value, T row)
  {
    super(timestamp, value);
    this.row = row;
  }
}
