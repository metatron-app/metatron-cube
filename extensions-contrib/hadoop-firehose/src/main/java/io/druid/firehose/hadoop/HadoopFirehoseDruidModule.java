package io.druid.firehose.hadoop;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;

import java.util.Arrays;
import java.util.List;

public class HadoopFirehoseDruidModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Arrays.<Module>asList(
        new SimpleModule("HadoopFirehoseModule").registerSubtypes(HadoopFirehoseFactory.class)
    );
  }

  @Override
  public void configure(Binder binder)
  {
  }
}
