package io.druid.guice;

import com.google.inject.Module;

import java.util.List;

public interface JerseyModule extends Module
{
  List<Class> getResources();
}
