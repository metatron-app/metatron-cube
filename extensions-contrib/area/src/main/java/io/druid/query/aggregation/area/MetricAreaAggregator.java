package io.druid.query.aggregation.area;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

import java.util.Comparator;

public class MetricAreaAggregator implements Aggregator
{
  public static final Comparator COMPARATOR = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      MetricArea ma1 = (MetricArea) o1;
      MetricArea ma2 = (MetricArea) o2;

      return Double.compare(ma1.getArea(), ma2.getArea());
    }
  };

  public static MetricArea combine(Object lma, Object rma)
  {
    return ((MetricArea)lma).add(rma);
  }

  private final ObjectColumnSelector selector;
  private MetricArea metricArea;

  public MetricAreaAggregator(
      ObjectColumnSelector selector
  )
  {
    this.selector = selector;
    this.metricArea = new MetricArea();
  }

  @Override
  public void aggregate()
  {
    metricArea.add(selector.get());
  }

  @Override
  public void reset()
  {
    metricArea.reset();
  }

  @Override
  public Object get()
  {
    return metricArea;
  }

  @Override
  public Float getFloat()
  {
    return (float)metricArea.getArea();
  }

  @Override
  public void close()
  {
  }

  @Override
  public Long getLong()
  {
    return (long) metricArea.getArea();
  }

  @Override
  public Double getDouble()
  {
    return metricArea.getArea();
  }
}
