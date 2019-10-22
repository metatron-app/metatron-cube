package io.druid.java.util.emitter.core.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.java.util.emitter.core.NoopEmitter;
import org.asynchttpclient.AsyncHttpClient;

public class NoopEmiterFactory implements EmitterFactory
{
  @Override
  public Emitter makeEmitter(ObjectMapper objectMapper, AsyncHttpClient httpClient, Lifecycle lifecycle)
  {
    return makeEmitter(lifecycle);
  }

  public Emitter makeEmitter(Lifecycle lifecycle)
  {
    Emitter retVal = new NoopEmitter();
    lifecycle.addManagedInstance(retVal);
    return retVal;
  }
}
