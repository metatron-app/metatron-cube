package io.druid.server;

import com.google.inject.BindingAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@BindingAnnotation
public @interface Shutdown
{
  interface Proc
  {
    void shutdown();

    boolean shutdown(long timeout);
  }
}
