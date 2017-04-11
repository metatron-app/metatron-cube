package io.druid.server.log;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import java.net.MalformedURLException;
import java.net.URL;

/**
 */
public class EventForwarder
{
  private static final Logger log = new Logger(EventForwarder.class);

  @JsonProperty
  private final URL defaultPostURL;
  @JsonProperty
  private final long defaultReadTimeout;

  private final HttpClient client;
  private final ObjectMapper jsonMapper;

  @JsonCreator
  public EventForwarder(
      @JsonProperty("defaultPostURL") String defaultPostURL,
      @JsonProperty("defaultReadTimeout") long defaultReadTimeout,
      @JacksonInject ObjectMapper jsonMapper,
      @JacksonInject Lifecycle lifecycle
  )
      throws MalformedURLException
  {
    this.defaultPostURL = defaultPostURL != null ? new URL(defaultPostURL) : null;
    this.defaultReadTimeout = defaultReadTimeout;
    this.jsonMapper = jsonMapper;
    this.client = HttpClientInit.createClient(HttpClientConfig.builder().withNumConnections(1).build(), lifecycle);
  }

  public void forward(String postURL, Object payload, Long readTimeout)
  {
    try {
      URL targetURL = Strings.isNullOrEmpty(postURL) ? defaultPostURL : new URL(postURL);
      if (targetURL != null) {
        long targetTimeout = readTimeout == null ? defaultReadTimeout : readTimeout;
        client.go(
            new Request(HttpMethod.POST, targetURL).setContent(
                "application/json",
                jsonMapper.writeValueAsBytes(payload)
            ),
            new StatusResponseHandler(Charsets.UTF_8),
            Duration.millis(targetTimeout)
        ).get();
      }
    }
    catch (Exception e) {
      log.warn(e, "failed to post event to %s", postURL == null ? defaultPostURL : postURL);
    }
  }
}
