package io.druid.query;

import com.google.common.base.Function;
import io.druid.data.input.Row;

import java.util.List;
import java.util.Map;

public interface Classifier extends Function<Map<String, Object>, Map<String, Object>>
{
  Function<Object[], Object[]> init(List<String> outputColumns);
}
