package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class BulkRow extends AbstractRow
{
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
    final int max = ((List) values[0]).size();
    return Sequences.simple(
        new Iterable<Row>()
        {
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
                  final Object value = ((List) values[i]).get(ix);
                  row[i] = value instanceof byte[] ? StringUtils.fromUtf8((byte[]) value) : value;
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
