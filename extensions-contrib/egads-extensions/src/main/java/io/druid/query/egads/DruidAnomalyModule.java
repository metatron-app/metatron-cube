package io.druid.query.egads;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;

import java.util.List;

/**
 */
public class DruidAnomalyModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("anomaly").registerSubtypes(
            AnomalyPostProcessor.class
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
  }
}
