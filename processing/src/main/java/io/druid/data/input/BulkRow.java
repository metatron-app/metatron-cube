package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.python.google.common.primitives.Ints;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class BulkRow extends AbstractRow
{
  private static final LZ4FastDecompressor LZ4 = LZ4Factory.fastestInstance().fastDecompressor();

  private final Object[] values;

  @JsonCreator
  public BulkRow(@JsonProperty("values") Object[] values)
  {
    this.values = values;
  }

  @JsonProperty
  public Object[] getValues()
  {
    return values;
  }

  public Sequence<Row> decompose()
  {
    for (int i = 0; i < values.length; i++) {
      if (values[i] instanceof byte[]) {
        final byte[] array = (byte[]) values[i];
        values[i] = new BytesInputStream(
            LZ4.decompress(array, Integer.BYTES, Ints.fromByteArray(array))
        );
      }
    }
    final int max = ((List) values[0]).size();
    return Sequences.simple(
        new Iterable<Row>()
        {
          {
            for (int i = 0; i < values.length; i++) {
              if (values[i] instanceof BytesInputStream) {
                ((BytesInputStream) values[i]).reset();
              }
            }
          }

          @Override
          public Iterator<Row> iterator()
          {
            return new Iterator<Row>()
            {
              private int index;

              @Override
              public boolean hasNext()
              {
                return index < max;
              }

              @Override
              public Row next()
              {
                final int ix = index++;
                final Object[] row = new Object[values.length];
                for (int i = 0; i < row.length; i++) {
                  if (values[i] instanceof BytesInputStream) {
                    row[i] = ((BytesInputStream) values[i]).readUTF();
                  } else {
                    row[i] = ((List) values[i]).get(ix);
                  }
                }
                return new CompactRow(row);
              }
            };
          }
        }
    );
  }

  @Override
  public Object getRaw(String dimension)
  {
    throw new UnsupportedOperationException("getRaw");
  }

  @Override
  public Collection<String> getColumns()
  {
    throw new UnsupportedOperationException("getColumns");
  }
}
