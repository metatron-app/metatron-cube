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

package io.druid.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.query.ResultWriter;
import io.druid.segment.indexing.DataSchema;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 */
public class BrokerLoadSpec
{
  private final String basePath;  // optional absolute path (paths in elements are regarded as relative to this)

  private final List<String> paths;
  private final String inputFormat; // todo

  private final DataSchema schema;

  @JsonCreator
  public BrokerLoadSpec(
      @JsonProperty("basePath") String basePath,
      @JsonProperty("paths") List<String> paths,
      @JsonProperty("inputFormat") String inputFormat,
      @JsonProperty("schema") DataSchema schema
  )
  {
    this.basePath = basePath;
    this.paths = Preconditions.checkNotNull(paths, "paths should not be null");
    this.inputFormat = inputFormat;
    this.schema = Preconditions.checkNotNull(schema, "schema should not be null");
    Preconditions.checkArgument(!paths.isEmpty(), "paths should not be empty");
  }

  @JsonProperty
  public String getBasePath()
  {
    return basePath;
  }

  @JsonProperty
  public List<String> getPaths()
  {
    return paths;
  }

  @JsonProperty
  public String getInputFormat()
  {
    return inputFormat;
  }

  @JsonProperty
  public DataSchema getSchema()
  {
    return schema;
  }

  public List<URI> getURIs() throws URISyntaxException
  {
    URI parent = basePath == null ? null : normalize(new URI(basePath));
    List<URI> uris = Lists.newArrayList();
    String prev = null;
    for (String path : paths) {
      URI child = resolve(parent, new URI(path));
      if (prev == null || prev.equals(child.getScheme())) {
        prev = child.getScheme();
        uris.add(child);
        continue;
      }
      throw new IllegalArgumentException("Conflicting schema " + prev + " and " + child.getScheme());
    }
    return uris;
  }

  private URI normalize(URI parent) throws URISyntaxException
  {
    // Add a slash to parent's path so resolution is compatible with URI's
    String parentPath = parent.getPath();
    if (!(parentPath.equals("/") || parentPath.isEmpty())) {
      parent = new URI(parent.getScheme(), parent.getAuthority(), parent.getPath() + "/", null, parent.getFragment());
    }
    if (parent.getScheme() == null) {
      parent = new URI(ResultWriter.FILE_SCHEME, parent.getAuthority(), parent.getPath(), parent.getFragment());
    }
    return parent.normalize();
  }

  private URI resolve(URI parent, URI child) throws URISyntaxException
  {
    URI resolved = parent == null ? child : parent.resolve(child);
    return new URI(
        resolved.getScheme() == null ? ResultWriter.FILE_SCHEME : resolved.getScheme(),
        resolved.getUserInfo(),
        resolved.getHost(),
        resolved.getPort(),
        resolved.getPath(),
        resolved.getQuery(),
        resolved.getFragment()
    ).normalize();
  }

  @Override
  public String toString()
  {
    return "BrokerLoadSpec{" +
           "basePath=" + basePath +
           ", elements=" + paths +
           ", inputFormat=" + inputFormat +
           ", schema=" + schema +
           '}';
  }
}
