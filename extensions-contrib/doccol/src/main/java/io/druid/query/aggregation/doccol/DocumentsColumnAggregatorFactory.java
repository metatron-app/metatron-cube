package io.druid.query.aggregation.doccol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@JsonTypeName("docColAgg")
public class DocumentsColumnAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0xc;
  protected final String name;
  protected final String fieldName;

  protected final boolean compress;

  @JsonCreator
  public DocumentsColumnAggregatorFactory(
    @JsonProperty("name") String name,
    @JsonProperty("fieldName") String fieldName,
    @JsonProperty("compress") Boolean compress
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.compress = compress == null ? true : compress;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new DocumentsColumnAggregator(
        metricFactory.makeObjectColumnSelector(fieldName),
        compress
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new DocumentsColumnBufferAggregator(
        metricFactory.makeObjectColumnSelector(fieldName),
        compress
    );
  }

  @Override
  public Comparator getComparator()
  {
    return DocumentsColumnAggregator.COMPARATOR;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner<DocumentsColumn> combiner()
  {
    return new Combiner<DocumentsColumn>()
    {
      @Override
      public DocumentsColumn combine(DocumentsColumn param1, DocumentsColumn param2)
      {
        return param1.add(param2);
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DocumentsColumnAggregatorFactory(name, name, compress);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof DocumentsColumnAggregatorFactory) {
      DocumentsColumnAggregatorFactory castedOther = (DocumentsColumnAggregatorFactory) other;

      return new DocumentsColumnAggregatorFactory(
          name,
          name,
          compress | castedOther.compress
      );

    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return DocumentsColumn.fromBytes((byte[])object);
    } else if (object instanceof ByteBuffer) {
      return DocumentsColumn.fromBytes((ByteBuffer)object);
    } else if (object instanceof String) {
      return  DocumentsColumn.fromBytes(Base64.decodeBase64(StringUtils.getBytesUtf8((String) object)));
    }
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((DocumentsColumn)object).get();
  }

  @Override
  public ValueDesc finalizedType()
  {
    return ValueDesc.STRING_ARRAY;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.getBytesUtf8(fieldName);
    return ByteBuffer.allocate(1 + fieldNameBytes.length + 1)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put(compress ? DocumentsColumn.COMPRESS_FLAG : 0).array();
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.of("docColumn");
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return DocumentsColumn.sizeMeta();
  }

}
