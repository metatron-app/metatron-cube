package io.druid.query.aggregation.range;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Doubles;
import io.druid.common.utils.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MetricRangeAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x32;
  protected final String name;
  protected final String fieldName;

  @JsonCreator
  public MetricRangeAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new MetricRangeAggregator(
        name,
        metricFactory.makeObjectColumnSelector(fieldName)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new MetricRangeBufferAggregator(
        metricFactory.makeObjectColumnSelector(fieldName)
    );
  }

  @Override
  public Comparator getComparator()
  {
    return MetricRangeAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return MetricRangeAggregator.combine(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new MetricRangeAggregatorFactory(name, name);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof MetricRangeAggregatorFactory) {
      return new MetricRangeAggregatorFactory(
          name,
          name
      );

    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return MetricRange.fromBytes((byte[])object);
    } else if (object instanceof ByteBuffer) {
      return MetricRange.fromBytes((ByteBuffer)object);
    } else if (object instanceof String) {
      return  MetricRange.fromBytes(Base64.decodeBase64(StringUtils.toUtf8((String) object)));
    }
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((MetricRange)object).getRange();
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    return ByteBuffer.allocate(1 + fieldNameBytes.length)
        .put(CACHE_TYPE_ID)
        .put(fieldNameBytes)
        .array();
  }

  @Override
  public String getTypeName()
  {
    return "metricRange";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Doubles.BYTES * 2;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return new MetricRange();
  }
}
