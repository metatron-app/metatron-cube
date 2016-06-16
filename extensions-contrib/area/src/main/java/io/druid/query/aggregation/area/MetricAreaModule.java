package io.druid.query.aggregation.area;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;
import io.druid.segment.serde.ComplexMetrics;

import java.util.List;

public class MetricAreaModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule().registerSubtypes(
            new NamedType(MetricAreaAggregatorFactory.class, "areaAgg"),
            new NamedType(MetricAreaPostAggregator.class, "areaPost")
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType("metricArea") == null) {
      ComplexMetrics.registerSerde("metricArea", new MetricAreaSerde());
    }
  }
}
