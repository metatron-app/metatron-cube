package io.druid.query.aggregation.doccol;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

import java.util.Comparator;

public class DocumentsColumnAggregator implements Aggregator
{
  public static final Comparator COMPARATOR = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      DocumentsColumn dc1 = (DocumentsColumn)o1;
      DocumentsColumn dc2 = (DocumentsColumn)o2;

      if (dc1.numDoc == dc2.numDoc)
      {
        return dc1.get().get(0).compareTo(dc2.get().get(0));
      }

      return Integer.compare(dc1.numDoc, dc2.numDoc);
    }
  };

  public static DocumentsColumn combine(Object ldc, Object rdc)
  {
    return ((DocumentsColumn)ldc).add((DocumentsColumn)rdc);
  }

  private final ObjectColumnSelector selector;
  private final boolean compress;
  private DocumentsColumn documentsColumn;

  public DocumentsColumnAggregator(
      ObjectColumnSelector selector,
      boolean compress
  )
  {
    this.selector = selector;
    this.compress = compress;

    this.documentsColumn = new DocumentsColumn(compress);
  }

  @Override
  public void aggregate() {
    Object object = selector.get();
    if (object instanceof String) {
      documentsColumn.add((String)object);
    } else if (object instanceof DocumentsColumn) {
      documentsColumn.add((DocumentsColumn) object);
    }
  }

  @Override
  public void reset() {
    this.documentsColumn = new DocumentsColumn(compress);
  }

  @Override
  public Object get() {
    return documentsColumn;
  }

  @Override
  public Float getFloat() {
    throw new UnsupportedOperationException("DocumentsColumnAggregator does not support getFloat()");
  }

  @Override
  public void close() {
    if (documentsColumn != null)
    {
      documentsColumn = null;
    }
  }

  @Override
  public Long getLong() {
    throw new UnsupportedOperationException("DocumentsColumnAggregator does not support getLong()");
  }

  @Override
  public Double getDouble()
  {
    throw new UnsupportedOperationException("DocumentsColumnAggregator does not support getDouble()");
  }
}
