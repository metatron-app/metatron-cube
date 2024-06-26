/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.emitter.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.core.factory.EmitterFactory;
import org.asynchttpclient.AsyncHttpClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Emitters
{
  private static final Logger log = new Logger(Emitters.class);

  private static final String LOG_EMITTER_PROP = "io.druid.java.util.emitter.logging";
  private static final String HTTP_EMITTER_PROP = "io.druid.java.util.emitter.http";
  private static final String CUSTOM_EMITTER_TYPE_PROP = "io.druid.java.util.emitter.type";

  public static Emitter create(Properties props, AsyncHttpClient httpClient, Lifecycle lifecycle)
  {
    return create(props, httpClient, new ObjectMapper(), lifecycle);
  }

  public static Emitter create(Properties props, AsyncHttpClient httpClient, ObjectMapper jsonMapper, Lifecycle lifecycle)
  {
    Map<String, Object> jsonified = Maps.newHashMap();
    if (props.getProperty(LOG_EMITTER_PROP) != null) {
      jsonified = makeLoggingMap(props);
      jsonified.put("type", "logging");
    }
    else if (props.getProperty(HTTP_EMITTER_PROP) != null) {
      jsonified = makeHttpMap(props);
      jsonified.put("type", "http");
    }
    else if (props.getProperty(CUSTOM_EMITTER_TYPE_PROP) !=null) {
      jsonified = makeCustomFactoryMap(props);
    }
    else {
      throw new ISE(
          "Unknown type of emitter. Please set [%s], [%s] or provide registered subtype of io.druid.java.util.emitter.core.factory.EmitterFactory via [%s]",
          LOG_EMITTER_PROP,
          HTTP_EMITTER_PROP,
          CUSTOM_EMITTER_TYPE_PROP
      );
    }
    return jsonMapper.convertValue(jsonified, EmitterFactory.class).makeEmitter(jsonMapper, httpClient, lifecycle);
  }

  // Package-visible for unit tests

  static Map<String, Object> makeHttpMap(Properties props)
  {
    Map<String, Object> httpMap = Maps.newHashMap();

    final String urlProperty = "io.druid.java.util.emitter.http.url";

    final String baseUrl = props.getProperty(urlProperty);
    if (baseUrl == null) {
      throw new IAE("Property[%s] must be set", urlProperty);
    }

    httpMap.put("recipientBaseUrl", baseUrl);
    httpMap.put("flushMillis", Long.parseLong(props.getProperty("io.druid.java.util.emitter.flushMillis", "60000")));
    httpMap.put("flushCount", Integer.parseInt(props.getProperty("io.druid.java.util.emitter.flushCount", "300")));
    /**
     * The defaultValue for "io.druid.java.util.emitter.http.flushTimeOut" must be same as {@link HttpEmitterConfig.DEFAULT_FLUSH_TIME_OUT}
     * */
    httpMap.put("flushTimeOut", Long.parseLong(props.getProperty("io.druid.java.util.emitter.http.flushTimeOut", String.valueOf(Long.MAX_VALUE))));
    if (props.containsKey("io.druid.java.util.emitter.http.basicAuthentication")) {
      httpMap.put("basicAuthentication", props.getProperty("io.druid.java.util.emitter.http.basicAuthentication"));
    }
    if (props.containsKey("io.druid.java.util.emitter.http.batchingStrategy")) {
      httpMap.put("batchingStrategy", props.getProperty("io.druid.java.util.emitter.http.batchingStrategy").toUpperCase());
    }
    if (props.containsKey("io.druid.java.util.emitter.http.maxBatchSize")) {
      httpMap.put("maxBatchSize", Integer.parseInt(props.getProperty("io.druid.java.util.emitter.http.maxBatchSize")));
    }
    if (props.containsKey("io.druid.java.util.emitter.http.batchQueueSizeLimit")) {
      httpMap.put("batchQueueSizeLimit", Integer.parseInt(props.getProperty("io.druid.java.util.emitter.http.batchQueueSizeLimit")));
    }
    if (props.containsKey("io.druid.java.util.emitter.http.httpTimeoutAllowanceFactor")) {
      httpMap.put("httpTimeoutAllowanceFactor", Float.parseFloat(props.getProperty("io.druid.java.util.emitter.http.httpTimeoutAllowanceFactor")));
    }
    if (props.containsKey("io.druid.java.util.emitter.http.minHttpTimeoutMillis")) {
      httpMap.put("minHttpTimeoutMillis", Float.parseFloat(props.getProperty("io.druid.java.util.emitter.http.minHttpTimeoutMillis")));
    }
    return httpMap;
  }

  // Package-visible for unit tests
  static Map<String, Object> makeLoggingMap(Properties props)
  {
    Map<String, Object> loggingMap = Maps.newHashMap();

    loggingMap.put(
        "loggerClass", props.getProperty("io.druid.java.util.emitter.logging.class", LoggingEmitter.class.getName())
    );
    loggingMap.put(
        "logLevel", props.getProperty("io.druid.java.util.emitter.logging.level", "debug")
    );
    return loggingMap;
  }

  static Map<String, Object> makeCustomFactoryMap(Properties props)
  {
    Map<String, Object> factoryMap = Maps.newHashMap();
    String prefix = "io.druid.java.util.emitter.";

    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      String key = entry.getKey().toString();
      if (key.startsWith(prefix)) {
        String combinedKey = key.substring(prefix.length());
        Map<String, Object> currentLevelJson = factoryMap;
        String currentKey = null;
        String[] keyPath = combinedKey.split("\\.");

        for (int i = 0; i < keyPath.length - 1; i++) {
          String keyPart = keyPath[i];
          Object nextLevelJson = currentLevelJson.get(keyPart);
          if (nextLevelJson == null) {
            nextLevelJson = new HashMap<String, Object>();
            currentLevelJson.put(keyPart, nextLevelJson);
          }
          currentLevelJson = (Map<String, Object>) nextLevelJson;
        }

        currentLevelJson.put(keyPath[keyPath.length - 1], entry.getValue());
      }
    }
    return factoryMap;
  }
}
