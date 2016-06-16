package io.druid.query.aggregation.doccol;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class DocumentsColumnBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector selector;
  private final boolean compress;
  private Map<Integer, DocumentsColumn> docMap;

  public DocumentsColumnBufferAggregator(
      ObjectColumnSelector selector,
      boolean compress
  )
  {
    this.selector = selector;
    this.compress = compress;
    this.docMap = new HashMap<>();
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.putInt(0);
    mutationBuffer.put(compress ? DocumentsColumn.COMPRESS_FLAG : 0);
    docMap.put(position, new DocumentsColumn(compress));
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    DocumentsColumn documentsColumn = docMap.get(position);
    Object object = selector.get();
    if (object instanceof String) {
      documentsColumn.add((String) object);
    } else if (object instanceof DocumentsColumn) {
      documentsColumn.add((DocumentsColumn) object);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position) {
    return docMap.get(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position) {
    throw new UnsupportedOperationException("DocumentColumnBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position) {
    throw new UnsupportedOperationException("DocumentColumnBufferAggregator does not support getLong()");
  }

  @Override
  public void close() {
  }
}
