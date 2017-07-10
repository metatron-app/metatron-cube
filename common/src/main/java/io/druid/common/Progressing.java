package io.druid.common;

import java.io.IOException;

/**
 */
public interface Progressing
{
  float progress() throws IOException, InterruptedException;
}
