package io.druid.query.aggregation.median;

import com.google.common.collect.Ordering;
import io.druid.data.input.Row;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public class DruidTDigestSerde extends ComplexMetricSerde
{
  private static Ordering<DruidTDigest> comparator = new Ordering<DruidTDigest>()
  {
    @Override
    public int compare(
        DruidTDigest arg1, DruidTDigest arg2
    )
    {
      return DruidTDigestAggregator.COMPARATOR.compare(arg1, arg2);
    }
  }.nullsFirst();

  @Override
  public String getTypeName()
  {
    return "DruidTDigest";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<DruidTDigest> extractedClass()
      {
        return DruidTDigest.class;
      }

      @Override
      public DruidTDigest extractValue(Row inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue instanceof DruidTDigest) {
          return (DruidTDigest) rawValue;
        } else {
          List<String> dimValues = inputRow.getDimension(metricName);
          if (dimValues != null && dimValues.size() > 0) {
            Iterator<String> values = dimValues.iterator();

            DruidTDigest digest = new DruidTDigest(DruidTDigestAggregator.DEFAULT_COMPRESSION);

            while (values.hasNext()) {
              double value = Double.parseDouble(values.next());
              digest.add(value);
            }
            return digest;
          } else {
            return new DruidTDigest(DruidTDigestAggregator.DEFAULT_COMPRESSION);
          }
        }
      }
    };
  }

  @Override
  public void deserializeColumn(
      ByteBuffer byteBuffer, ColumnBuilder columnBuilder
  )
  {
    final GenericIndexed column = GenericIndexed.read(byteBuffer, getObjectStrategy());
    columnBuilder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<DruidTDigest>()
    {
      @Override
      public Class<? extends DruidTDigest> getClazz()
      {
        return DruidTDigest.class;
      }

      @Override
      public DruidTDigest fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        return DruidTDigest.fromBytes(readOnlyBuffer);
      }

      @Override
      public byte[] toBytes(DruidTDigest digest)
      {
        if (digest == null) {
          return new byte[]{};
        }

        return digest.toBytes();
      }

      @Override
      public int compare(DruidTDigest o1, DruidTDigest o2)
      {
        return comparator.compare(o1, o2);
      }
    };
  }
}
