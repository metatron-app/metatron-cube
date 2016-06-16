package io.druid.query.aggregation.median;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.primitives.Ints;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.codec.binary.StringUtils;

import java.nio.ByteBuffer;

@JsonTypeName("digestQuantileCombineAgg")
public class DruidTDigestCombiningAggregatorFactory extends DruidTDigestAggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0xB;

  private static ObjectColumnSelector DEFAULT_SELECTOR = new ObjectColumnSelector<DruidTDigest>() {
    @Override
    public Class<DruidTDigest> classOfObject()
    {
      return DruidTDigest.class;
    }

    @Override
    public DruidTDigest get()
    {
      return new DruidTDigest(DruidTDigestAggregator.DEFAULT_COMPRESSION);
    }
  };

  @JsonCreator
  public DruidTDigestCombiningAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("compression") Integer compression
  )
  {
    super(name, fieldName, compression);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

    if (selector == null) {
      // gracefully handle undefined metrics
      selector = DEFAULT_SELECTOR;
    }

    final Class cls = selector.classOfObject();
    if (cls.equals(Object.class) || DruidTDigest.class.isAssignableFrom(cls)) {
      return new DruidTDigestCombiningAggregator(
          name,
          selector,
          compression
      );
    }

    throw new IllegalArgumentException(
        String.format("Incompatible type for metric[%s], expected a DruidTDigest, got a %s", fieldName, cls)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

    if (selector == null) {
      // gracefully handle undefined metrics
      selector = DEFAULT_SELECTOR;
    }

    final Class cls = selector.classOfObject();
    if (cls.equals(Object.class) || DruidTDigest.class.isAssignableFrom(cls)) {
      return new DruidTDigestCombiningBufferAggregator(
          selector,
          compression
      );
    }

    throw new IllegalArgumentException(
        String.format("Incompatible type for metric[%s], expected a ApproximateHistogram, got a %s", fieldName, cls)
    );
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DruidTDigestCombiningAggregatorFactory(name, fieldName, compression);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.getBytesUtf8(fieldName);
    return ByteBuffer.allocate(1 + fieldNameBytes.length + Ints.BYTES)
        .put(CACHE_TYPE_ID)
        .put(fieldNameBytes)
        .putInt(compression).array();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DruidTDigestCombiningAggregatorFactory that = (DruidTDigestCombiningAggregatorFactory) o;

    if (compression != that.compression) {
      return false;
    }
    if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public String toString()
  {
    return "DruidTDigestCombiningAggregatorFactory{" +
        "name='" + name + '\'' +
        ", fieldName='" + fieldName + '\'' +
        ", compression=" + compression +
        '}';
  }
}
