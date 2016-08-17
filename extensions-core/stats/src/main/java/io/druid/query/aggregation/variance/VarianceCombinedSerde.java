package io.druid.query.aggregation.variance;

import io.druid.data.input.InputRow;
import io.druid.segment.serde.ComplexMetricExtractor;

import java.util.List;

public class VarianceCombinedSerde extends VarianceSerde
{
  @Override
  public String getTypeName()
  {
    return "varianceCombined";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<VarianceAggregatorCollector> extractedClass()
      {
        return VarianceAggregatorCollector.class;
      }

      @Override
      public VarianceAggregatorCollector extractValue(InputRow inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue instanceof VarianceAggregatorCollector) {
          return (VarianceAggregatorCollector) rawValue;
        } else if (rawValue instanceof String) {
          String strValue = (String)rawValue;
          String[] params = strValue.split(",");
          if (params.length == 3) {
            double nvar = Double.parseDouble(params[0].trim());
            long count = Long.parseLong(params[1].trim());
            double sum = Double.parseDouble(params[2].trim());
            return new VarianceAggregatorCollector(count, sum, nvar);
          }
        }
        VarianceAggregatorCollector collector = new VarianceAggregatorCollector();

        List<String> dimValues = inputRow.getDimension(metricName);
        if (dimValues != null && dimValues.size() > 0) {
          for (String dimValue : dimValues) {
            double value = Double.parseDouble(dimValue);
            collector.add(value);
          }
        }
        return collector;
      }
    };
  }
}
