package io.druid.query;

import com.metamx.common.Pair;
import com.metamx.common.guava.Sequence;

import java.util.Map;

/**
 */
public interface UnionAllQueryRunner<T>
{
  Sequence<Pair<Query<T>, Sequence<T>>> run(Query<T> query, Map<String, Object> responseContext);
}
