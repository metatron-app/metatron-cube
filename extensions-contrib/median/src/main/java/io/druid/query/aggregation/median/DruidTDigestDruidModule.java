package io.druid.query.aggregation.median;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;
import io.druid.segment.serde.ComplexMetrics;

import java.util.List;

public class DruidTDigestDruidModule implements DruidModule{
  @Override
  public List<? extends Module> getJacksonModules() {
    return ImmutableList.of(
      new SimpleModule().registerSubtypes(
          DruidTDigestAggregatorFactory.class,
          DruidTDigestMedianPostAggregator.class,
          DruidTDigestQuantilePostAggregator.class,
          DruidTDigestQuantilesPostAggregator.class
      )
    );
  }

  @Override
  public void configure(Binder binder) {
    if (ComplexMetrics.getSerdeForType("DruidTDigest") == null) {
      ComplexMetrics.registerSerde("DruidTDigest", new DruidTDigestSerde());
    }

  }
}
