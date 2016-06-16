package io.druid.query.aggregation.range;

import com.google.common.collect.Ordering;
import io.druid.data.input.InputRow;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;

public class MetricRangeSerde extends ComplexMetricSerde
{
  private static Ordering<MetricRange> comparator = new Ordering<MetricRange>() {
    @Override
    public int compare(MetricRange mr1, MetricRange mr2) {
      return MetricRangeAggregator.COMPARATOR.compare(mr1, mr2);
    }
  }.nullsFirst();

  private ObjectStrategy strategy = new ObjectStrategy<MetricRange>() {
    @Override
    public Class getClazz() {
      return MetricRange.class;
    }

    @Override
    public MetricRange fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
      readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
      return MetricRange.fromBytes(readOnlyBuffer);
    }

    @Override
    public byte[] toBytes(MetricRange val)
    {
      if (val == null) {
        return new byte[]{};
      }
      return val.toBytes();
    }

    @Override
    public int compare(MetricRange o1, MetricRange o2)
    {
      return comparator.compare(o1, o2);
    }
  };

  @Override
  public String getTypeName()
  {
    return "metricRange";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<?> extractedClass()
      {
        return MetricRange.class;
      }

      @Override
      public MetricRange extractValue(InputRow inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue instanceof MetricRange) {
          return (MetricRange) rawValue;
        } else {
          return new MetricRange().add(inputRow.getFloatMetric(metricName));
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
