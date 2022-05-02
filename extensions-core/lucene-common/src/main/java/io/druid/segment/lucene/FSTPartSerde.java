package io.druid.segment.lucene;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Throwables;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.FSTHolder;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.serde.ColumnPartSerde;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.LuceneIndexInput;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;

import java.io.IOException;
import java.nio.ByteBuffer;

@JsonTypeName("fst.lucene")
public class FSTPartSerde implements ColumnPartSerde
{
  @Override
  public Deserializer getDeserializer()
  {
    return new Deserializer()
    {
      @Override
      public void read(ByteBuffer buffer, ColumnBuilder builder, BitmapSerdeFactory factory) throws IOException
      {
        final ByteBuffer block = ByteBufferSerializer.prepareForRead(buffer);
        ColumnPartProvider<FSTHolder> rFST = new ColumnPartProvider<FSTHolder>()
        {
          @Override
          public int numRows()
          {
            return builder.getNumRows();
          }

          @Override
          public long getSerializedSize()
          {
            return block.remaining();
          }

          @Override
          public FSTHolder get()
          {
            DataInput input = LuceneIndexInput.newInstance("FST", block.slice(), block.remaining());  // slice !
            try {
              // keep in heap.. need to upgrade lucene 8.x to avoid this
              final FST<Long> fst = new FST<>(input, PositiveIntOutputs.getSingleton());
              return new FSTHolder()
              {
                @Override
                public long occupation()
                {
                  return fst.ramBytesUsed();
                }

                @Override
                @SuppressWarnings("unchecked")
                public <T> T unwrap(Class<T> clazz)
                {
                  return clazz == FST.class ? (T) fst : null;
                }
              };
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        };
      }
    };
  }
}
