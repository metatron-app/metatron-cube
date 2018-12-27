package io.druid.segment;

import io.druid.query.select.Schema;

public interface SchemaProvider
{
  Schema asSchema(boolean prependTime);
}
