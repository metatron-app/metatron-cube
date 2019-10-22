package io.druid.java.util.emitter.core.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.java.util.emitter.core.LoggingEmitter;
import io.druid.java.util.emitter.core.LoggingEmitterConfig;
import org.asynchttpclient.AsyncHttpClient;

public class LoggingEmitterFactory extends LoggingEmitterConfig implements EmitterFactory
{
  public LoggingEmitterFactory() {}

  @Override
  public Emitter makeEmitter(ObjectMapper objectMapper, AsyncHttpClient httpClient, Lifecycle lifecycle)
  {
    return makeEmitter(objectMapper, lifecycle);
  }

  public Emitter makeEmitter(ObjectMapper objectMapper, Lifecycle lifecycle)
  {
    Emitter retVal = new LoggingEmitter(this, objectMapper);
    lifecycle.addManagedInstance(retVal);
    return retVal;
  }
}
