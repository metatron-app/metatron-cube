package io.druid.java.util.emitter.core.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.java.util.emitter.core.HttpEmitterConfig;
import io.druid.java.util.emitter.core.HttpPostEmitter;
import org.asynchttpclient.AsyncHttpClient;

public class HttpEmitterFactory extends HttpEmitterConfig implements EmitterFactory
{

  @Override
  public Emitter makeEmitter(ObjectMapper objectMapper, AsyncHttpClient httpClient, Lifecycle lifecycle)
  {
    Emitter retVal = new HttpPostEmitter(this, httpClient, objectMapper);
    lifecycle.addManagedInstance(retVal);
    return retVal;
  }
}
