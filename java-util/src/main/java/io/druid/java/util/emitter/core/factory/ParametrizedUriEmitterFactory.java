package io.druid.java.util.emitter.core.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.java.util.emitter.core.ParametrizedUriEmitter;
import io.druid.java.util.emitter.core.ParametrizedUriEmitterConfig;
import org.asynchttpclient.AsyncHttpClient;

public class ParametrizedUriEmitterFactory extends ParametrizedUriEmitterConfig implements EmitterFactory
{

  @Override
  public Emitter makeEmitter(ObjectMapper objectMapper, AsyncHttpClient httpClient, Lifecycle lifecycle)
  {
    final Emitter retVal = new ParametrizedUriEmitter(this, httpClient, objectMapper);
    lifecycle.addManagedInstance(retVal);
    return retVal;
  }
}
