package io.druid.query.aggregation.range;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;
import io.druid.segment.serde.ComplexMetrics;

import java.util.List;

public class MetricRangeModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule().registerSubtypes(
            new NamedType(MetricRangeAggregatorFactory.class, "rangeAgg"),
            new NamedType(MetricRangePostAggregator.class, "rangePost")
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType("metricRange") == null) {
      ComplexMetrics.registerSerde("metricRange", new MetricRangeSerde());
    }
  }
}
