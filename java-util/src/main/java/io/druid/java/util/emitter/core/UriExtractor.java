package io.druid.java.util.emitter.core;

import java.net.URI;
import java.net.URISyntaxException;

public interface UriExtractor
{
  URI apply(Event event) throws URISyntaxException;
}
