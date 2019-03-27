package io.druid.server;

import io.druid.query.QuerySegmentWalker;
import io.druid.query.StorageHandler;

public interface ForwardingSegmentWalker extends QuerySegmentWalker.Wrapper
{
  StorageHandler getHandler(String scheme);
}
