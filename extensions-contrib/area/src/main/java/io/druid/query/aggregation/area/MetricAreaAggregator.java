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

  private final String name;
  private final ObjectColumnSelector selector;
  private MetricArea metricArea;

  public MetricAreaAggregator(
      String name,
      ObjectColumnSelector selector
  )
  {
    this.name = name;
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
  public float getFloat()
  {
    return (float)metricArea.getArea();
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void close()
  {
  }

  @Override
  public long getLong()
  {
    return (long) metricArea.getArea();
  }

  @Override
  public double getDouble()
  {
    return (double) metricArea.getArea();
  }
}
