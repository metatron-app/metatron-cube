package io.druid.query.groupby;

import com.metamx.common.guava.Sequence;
import io.druid.data.input.Row;

import java.io.Closeable;

public interface MergeIndex extends Closeable
{
  void add(Row row);

  Sequence<Row> toMergeStream(boolean compact);
}
