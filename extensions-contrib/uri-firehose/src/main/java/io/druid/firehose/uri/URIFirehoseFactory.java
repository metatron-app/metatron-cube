/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.firehose.uri;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.RetryUtils;
import com.metamx.common.parsers.ParseException;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.impl.FileIteratingFirehose;
import io.druid.data.input.impl.InputRowParser;
import io.druid.segment.loading.URIDataPuller;
import org.apache.commons.io.LineIterator;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

public class URIFirehoseFactory implements FirehoseFactory
{
  private static final EmittingLogger log = new EmittingLogger(URIFirehoseFactory.class);

  private final URI uri;
  private final String pattern;
  private final Pattern compiledPattern;
  private final Map<String, SearchableVersionedDataFinder> pullers;

  @JsonCreator
  public URIFirehoseFactory(
      @JsonProperty("URI") URI uri,
      @JsonProperty("pattern") String pattern,
      @JacksonInject Map<String, SearchableVersionedDataFinder> pullers
  )
  {
    this.uri = Preconditions.checkNotNull(uri, "URI should be specified");
    this.pattern = pattern;
    this.compiledPattern = (pattern == null) ? null : Pattern.compile(pattern);
    Map<String, SearchableVersionedDataFinder> pullerMap = Maps.newHashMap();
    pullerMap.put("http", new HttpPuller());
    pullerMap.putAll(pullers);
    this.pullers = ImmutableMap.copyOf(pullerMap);
  }

  @JsonProperty
  public URI getUri()
  {
    return uri;
  }

  @JsonProperty
  public String getPattern()
  {
    return pattern;
  }

  @Override
  public Firehose connect(InputRowParser parser) throws IOException, ParseException
  {
    log.info("Connecting to %s", uri);

    final SearchableVersionedDataFinder<URI> pullerRaw = pullers.get(uri.getScheme());
    if (pullerRaw == null) {
      throw new IAE(
          "Unknown loader format[%s].  Known types are %s",
          uri.getScheme(),
          pullers.keySet()
      );
    }
    if (!(pullerRaw instanceof URIDataPuller)) {
      throw new IAE(
          "Cannot load data from location [%s]. Data pulling from [%s] not supported",
          uri,
          uri.getScheme()
      );
    }
    final URIDataPuller puller = (URIDataPuller) pullerRaw;
    final List<URI> uriList = pullerRaw.getAllVersions(uri, compiledPattern);

    if (uriList.isEmpty()) {
      throw new ISE("No file to ingest in %s", uri);
    }

    final Iterator<URI> uriIterator = uriList.iterator();

    return new FileIteratingFirehose(
        new Iterator<LineIterator>()
        {
          @Override
          public boolean hasNext()
          {
            return uriIterator.hasNext();
          }

          @Override
          public LineIterator next()
          {
            try {
              InputStream stream = puller.getInputStream(uriIterator.next());
              return new LineIterator(new BufferedReader(new InputStreamReader(stream, "UTF-8")));
            } catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        },
        parser
    );
  }

  // implement only needed in URIFirehoseFactory
  private class HttpPuller implements SearchableVersionedDataFinder<URI>, URIDataPuller
  {
    public static final int DEFAULT_RETRY_COUNT = 3;
    InputStream stream = null;

    // should be called after getAllVersions
    @Override
    public InputStream getInputStream(URI uri) throws IOException
    {
      if (stream == null) {
        checkAvailable(uri);
      }
      return stream;
    }

    @Override
    public String getVersion(URI uri) throws IOException
    {
      return null;
    }

    @Override
    public Predicate<Throwable> shouldRetryPredicate()
    {
      return new Predicate<Throwable>()
      {
        @Override
        public boolean apply(Throwable input)
        {
          if (input == null) {
            return false;
          }
          if (input instanceof MalformedURLException) {
            return false;
          }
          if (input instanceof IOException) {
            return true;
          }
          return apply(input.getCause());
        }
      };
    }

    @Override
    public URI getLatestVersion(URI uri, @Nullable Pattern pattern)
    {
      List<URI> uriList = getAllVersions(uri, pattern);
      return uriList.isEmpty() ? null : uriList.get(0);
    }

    @Override
    public List<URI> getAllVersions(final URI uri, final @Nullable Pattern pattern)
    {
      List<URI> uriList = Lists.newArrayListWithCapacity(1);

      if (stream != null) {
        uriList.add(uri);
      } else {
        try {
          URI filtered = RetryUtils.retry(
              new Callable<URI>()
              {
                @Override
                public URI call() throws Exception
                {
                  return checkAvailable(uri);
                }
              },
              shouldRetryPredicate(),
              DEFAULT_RETRY_COUNT
          );
          if (filtered != null) {
            uriList.add(filtered);
          }
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      return uriList;
    }

    @Override
    public Class<URI> getDataDescriptorClass()
    {
      return URI.class;
    }

    private URI checkAvailable(URI uri) throws IOException
    {
      URL url = uri.toURL();
      stream = url.openStream();

      return uri;
    }
  }
}
