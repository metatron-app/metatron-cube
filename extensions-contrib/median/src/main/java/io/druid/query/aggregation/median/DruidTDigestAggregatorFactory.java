package io.druid.query.aggregation.median;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@JsonTypeName("digestQuantileAgg")
public class DruidTDigestAggregatorFactory extends AggregatorFactory{
  private static final byte CACHE_TYPE_ID = 0xA;

  protected final String name;
  protected final String fieldName;
  protected final int compression;

  public DruidTDigestAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("compression") Integer compression
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.compression = compression == null ? DruidTDigestAggregator.DEFAULT_COMPRESSION : compression;

    Preconditions.checkArgument(this.compression > 0, "compression must be greater than 0");
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory) {
    return new DruidTDigestAggregator(
        name,
        metricFactory.makeFloatColumnSelector(fieldName),
        compression
        );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
    return new DruidTDigestBufferAggregator(
        metricFactory.makeFloatColumnSelector(fieldName),
        compression
    );
  }

  @Override
  public Comparator getComparator() {
    return DruidTDigestAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs) {
    ((DruidTDigest)lhs).add((DruidTDigest)rhs);
    return lhs;
  }

  @Override
  public AggregatorFactory getCombiningFactory() {
    return new DruidTDigestCombiningAggregatorFactory(name, name, compression);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns() {
    return Arrays.<AggregatorFactory>asList(
        new DruidTDigestAggregatorFactory(
            fieldName,
            fieldName,
            compression
        )
    );
  }

  @Override
  public Object deserialize(Object object) {
    if (object instanceof byte[]) {
      final DruidTDigest digest = DruidTDigest.fromBytes(ByteBuffer.wrap((byte[]) object));

      return digest;
    } else if (object instanceof ByteBuffer) {
      final DruidTDigest digest = DruidTDigest.fromBytes((ByteBuffer) object);

      return digest;
    } else if (object instanceof String) {
      byte[] bytes = Base64.decodeBase64(StringUtils.getBytesUtf8((String) object));
      final DruidTDigest digest = DruidTDigest.fromBytes(ByteBuffer.wrap(bytes));

      return digest;
    } else {
      return object;
    }
  }

  @Override
  public Object finalizeComputation(Object object) {
    // just return DruidTDigest object itself
    return object;
  }

  @JsonProperty
  @Override
  public String getName() {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public int getCompression()
  {
    return compression;
  }

  @Override
  public List<String> requiredFields() {
    return Arrays.asList(fieldName);
  }

  @Override
  public byte[] getCacheKey() {
    byte[] fieldNameBytes = StringUtils.getBytesUtf8(fieldName);
    return ByteBuffer.allocate(1 + fieldNameBytes.length + Ints.BYTES)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .putInt(compression).array();
  }

  @Override
  public String getTypeName() {
    return "DruidTDigest";
  }

  @Override
  public int getMaxIntermediateSize() {
    // NOTE: compression threshold is set as compression * 100 in TDigest
    return DruidTDigest.maxStorageSize(compression);
  }

  @Override
  public Object getAggregatorStartValue() {
    return new DruidTDigest(compression);
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

    DruidTDigestAggregatorFactory that = (DruidTDigestAggregatorFactory) o;

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
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
    result = 31 * result + compression;

    return result;
  }

  @Override
  public String toString()
  {
    return "ApproximateQuantileAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", compression=" + compression +
           '}';
  }
}
