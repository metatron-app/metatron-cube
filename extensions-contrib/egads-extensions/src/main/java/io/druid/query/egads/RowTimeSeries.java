package io.druid.query.egads;

import com.yahoo.egads.data.TimeSeries;

import java.util.List;

/**
 */
public class RowTimeSeries extends TimeSeries
{
  public RowTimeSeries(List<Entry> sequence)
  {
    data.addAll(sequence);
  }
}
