package io.druid.query.aggregation.area;

import com.google.common.collect.Ordering;
import io.druid.data.input.InputRow;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;

public class MetricAreaSerde extends ComplexMetricSerde
{
  private static Ordering<MetricArea> comparator = new Ordering<MetricArea>() {
    @Override
    public int compare(MetricArea ma1, MetricArea ma2) {
      return MetricAreaAggregator.COMPARATOR.compare(ma1, ma2);
    }
  }.nullsFirst();

  private ObjectStrategy strategy = new ObjectStrategy<MetricArea>() {
    @Override
    public Class getClazz() {
      return MetricArea.class;
    }

    @Override
    public MetricArea fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
      readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
      return MetricArea.fromBytes(readOnlyBuffer);
    }

    @Override
    public byte[] toBytes(MetricArea val)
    {
      if (val == null) {
        return new byte[]{};
      }
      return val.toBytes();
    }

    @Override
    public int compare(MetricArea o1, MetricArea o2)
    {
      return comparator.compare(o1, o2);
    }
  };

  @Override
  public String getTypeName()
  {
    return "metricArea";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<?> extractedClass()
      {
        return MetricArea.class;
      }

      @Override
      public MetricArea extractValue(InputRow inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue instanceof MetricArea) {
          return (MetricArea) rawValue;
        } else {
          return new MetricArea().add(inputRow.getFloatMetric(metricName));
        }
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    final GenericIndexed column = GenericIndexed.read(buffer, getObjectStrategy());
    builder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return strategy;
  }
}
