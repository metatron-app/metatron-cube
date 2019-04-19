package io.druid.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import io.druid.guice.JerseyModule;
import io.druid.server.http.ShutdownResource;

import java.util.List;

public class ShutdownModule implements JerseyModule
{
  private final Shutdown.Proc closeable;

  public ShutdownModule(Shutdown.Proc closeable) {this.closeable = closeable;}

  @Override
  public void configure(Binder binder)
  {
    binder.bind(Key.get(Shutdown.Proc.class, Shutdown.class)).toInstance(closeable);
  }

  @Override
  public List<Class> getResources()
  {
    return ImmutableList.<Class>of(ShutdownResource.class);
  }
}
