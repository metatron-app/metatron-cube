package io.druid.query.aggregation.doccol;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;
import io.druid.segment.serde.ComplexMetrics;

import java.util.List;

public class DocumentsColumnModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule().registerSubtypes(
            DocumentsColumnAggregatorFactory.class,
            DocumentsColumnPostAggregator.class
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType("docColumn") == null) {
      ComplexMetrics.registerSerde("docColumn", new DocumentsColumnSerde());
    }
  }
}
