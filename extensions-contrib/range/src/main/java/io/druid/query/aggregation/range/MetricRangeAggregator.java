package io.druid.query.aggregation.range;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

import java.util.Comparator;

public class MetricRangeAggregator implements Aggregator
{
  public static final Comparator COMPARATOR = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      MetricRange mr1 = (MetricRange)o1;
      MetricRange mr2 = (MetricRange)o2;

      return Double.compare(mr1.getRange(), mr2.getRange());
    }
  };

  public static MetricRange combine(Object lmr, Object rmr)
  {
    return ((MetricRange)lmr).add(rmr);
  }

  private final ObjectColumnSelector selector;
  private MetricRange metricRange;

  public MetricRangeAggregator(
      ObjectColumnSelector selector
  )
  {
    this.selector = selector;
    this.metricRange = new MetricRange();
  }

  @Override
  public void aggregate()
  {
    metricRange.add(selector.get());
  }

  @Override
  public void reset()
  {
    metricRange.reset();
  }

  @Override
  public Object get()
  {
    return metricRange;
  }

  @Override
  public Float getFloat()
  {
    return (float)metricRange.getRange();
  }

  @Override
  public void close()
  {

  }

  @Override
  public Long getLong()
  {
    return (long)metricRange.getRange();
  }

  @Override
  public Double getDouble()
  {
    return metricRange.getRange();
  }
}
