/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segwriter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import io.druid.timeline.DataSegment;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Publishes built segments to the overlord's POST /druid/indexer/v1/segments/publish endpoint
 * (the lock-free bulk publish). Pure JDK HTTP so it carries no extra deps onto the Spark driver.
 */
public final class SegmentPublisher
{
  private SegmentPublisher() {}

  /**
   * @param publishUrl full overlord publish URL (…/druid/indexer/v1/segments/publish)
   * @return number of segments the overlord reported as published
   */
  public static int publish(String publishUrl, List<DataSegment> segments, ObjectMapper mapper) throws IOException
  {
    final byte[] body = mapper.writeValueAsBytes(segments);
    final HttpURLConnection con = (HttpURLConnection) new URL(publishUrl).openConnection();
    con.setRequestMethod("POST");
    con.setRequestProperty("Content-Type", "application/json");
    con.setConnectTimeout(30_000);
    con.setReadTimeout(120_000);
    con.setDoOutput(true);
    try (OutputStream os = con.getOutputStream()) {
      os.write(body);
    }
    final int code = con.getResponseCode();
    final InputStream is = code < 400 ? con.getInputStream() : con.getErrorStream();
    final String resp = is == null ? "" : new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8);
    if (code >= 400) {
      throw new IOException("publish failed: HTTP " + code + " - " + resp);
    }
    final Map<String, Object> parsed = mapper.readValue(resp, Map.class);
    final Object published = parsed.get("published");
    return published instanceof Number ? ((Number) published).intValue() : segments.size();
  }
}
