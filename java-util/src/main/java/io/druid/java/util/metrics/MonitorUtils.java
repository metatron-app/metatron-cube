package io.druid.java.util.metrics;

import io.druid.java.util.emitter.service.ServiceMetricEvent;
import java.util.Map;

public class MonitorUtils
{
  public static void addDimensionsToBuilder(ServiceMetricEvent.Builder builder, Map<String, String[]> dimensions)
  {
    for (Map.Entry<String, String[]> keyValue : dimensions.entrySet()) {
      builder.setDimension(keyValue.getKey(), keyValue.getValue());
    }
  }
}
