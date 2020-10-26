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

package io.druid.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.Pair;
import io.druid.data.input.ReadConstants;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.data.input.Validation;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.output.ForwardConstants;
import io.druid.initialization.Initialization;
import io.druid.jackson.ObjectMappers;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.BaseQuery;
import io.druid.query.DummyQuery;
import io.druid.query.ForwardingSegmentWalker;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.StorageHandler;
import io.druid.segment.incremental.BaseTuningConfig;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.GranularitySpec;
import org.joda.time.Interval;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 */
public class FileLoadSpec implements ForwardConstants, ReadConstants
{
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format")
  public static interface Resolver
  {
    FileLoadSpec resolve(QuerySegmentWalker walker) throws IOException;
  }

  private final String basePath;  // optional absolute path (paths in elements are regarded as relative to this)

  private final List<String> paths;
  private final String inputFormat; // todo
  private final String extension;

  private final DataSchema schema;
  private final BaseTuningConfig tuningConfig;
  private final Boolean overwrite;
  private final Boolean temporary;

  private final Map<String, Object> properties;

  @JsonCreator
  public FileLoadSpec(
      @JsonProperty("basePath") String basePath,
      @JsonProperty("paths") List<String> paths,
      @JsonProperty("extension") String extension,
      @JsonProperty("inputFormat") String inputFormat,
      @JsonProperty("schema") DataSchema schema,
      @JsonProperty("temporary") Boolean temporary,
      @JsonProperty("overwrite") Boolean overwrite,
      @JsonProperty("tuningConfig") BaseTuningConfig tuningConfig,
      @JsonProperty("properties") Map<String, Object> properties
  )
  {
    this.basePath = basePath;
    this.paths = Preconditions.checkNotNull(paths, "paths should not be null");
    this.extension = extension;
    this.inputFormat = inputFormat;
    this.schema = Preconditions.checkNotNull(schema, "schema should not be null");
    this.overwrite = overwrite;
    this.temporary = temporary;
    this.tuningConfig = tuningConfig;
    this.properties = properties == null ? ImmutableMap.<String, Object>of() : properties;
    Preconditions.checkArgument(!paths.isEmpty(), "paths should not be empty");
    Preconditions.checkArgument(isTemporary() || !isOverwrite(), "cannot overwrite non-temporary table");
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
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
  @JsonInclude(Include.NON_NULL)
  public String getInputFormat()
  {
    return inputFormat;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getExtension()
  {
    return extension;
  }

  @JsonProperty
  public DataSchema getSchema()
  {
    return schema;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public BaseTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty
  public boolean isOverwrite()
  {
    return overwrite == null || overwrite;
  }

  @JsonProperty
  public boolean isTemporary()
  {
    return temporary == null || temporary;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public Map<String, Object> getProperties()
  {
    return properties;
  }

  @JsonIgnore
  private InputRowParser getParser(ObjectMapper mapper)
  {
    final boolean ignoreInvalidRows = tuningConfig != null && tuningConfig.isIgnoreInvalidRows();
    final Interval interval = schema.getGranularitySpec().umbrellaInterval();
    if (interval == null) {
      return schema.getParser(mapper, ignoreInvalidRows);
    }
    final Validation validation = Validation.expr(
        String.format("!between(__time, %d, %d)", interval.getStartMillis(), interval.getEndMillis() - 1)
    );
    return schema.withValidations(GuavaUtils.concat(schema.getValidations(), validation))
                 .getParser(mapper, ignoreInvalidRows);
  }

  @JsonIgnore
  public List<URI> getURIs()
  {
    List<URI> uris = Lists.newArrayList();
    try {
      URI parent = basePath == null ? null : normalize(new URI(basePath));
      String prev = null;
      for (String path : paths) {
        URI child = resolve(parent, new URI(path));
        if (prev == null || prev.equals(child.getScheme())) {
          prev = child.getScheme();
          uris.add(child);
          continue;
        }
        throw new IAE("conflicting schema %s and %s", prev, child.getScheme());
      }
    }
    catch (URISyntaxException e) {
      throw new IAE(e, "invalid URL %s", basePath);
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
      parent = new URI(StorageHandler.FILE_SCHEME, parent.getAuthority(), parent.getPath(), parent.getFragment());
    }
    return parent.normalize();
  }

  private URI resolve(URI parent, URI child) throws URISyntaxException
  {
    final URI resolved = parent == null ? child : parent.resolve(child);
    return new URI(
        Optional.fromNullable(resolved.getScheme()).or(StorageHandler.FILE_SCHEME),
        resolved.getUserInfo(),
        resolved.getHost(),
        resolved.getPort(),
        resolved.getPath(),
        resolved.getQuery(),
        resolved.getFragment()
    ).normalize();
  }

  public Pair<Query, Sequence> readFrom(ForwardingSegmentWalker walker) throws IOException
  {
    final ClassLoader prev = FileLoadSpec.class.getClassLoader();
    final ClassLoader loader = Initialization.getClassLoaderForExtension(extension, prev);
    Thread.currentThread().setContextClassLoader(loader);
    try {
      return read(walker);
    }
    finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
  }

  @SuppressWarnings("unchecked")
  private Pair<Query, Sequence> read(ForwardingSegmentWalker walker) throws IOException
  {
    final List<URI> locations = getURIs();
    final String scheme = locations.get(0).getScheme();
    final StorageHandler handler = walker.getHandler(scheme);
    if (handler == null) {
      throw new IAE("Unsupported scheme '%s'", scheme);
    }
    final ObjectMapper jsonMapper = walker.getObjectMapper();

    final InputRowParser parser = getParser(jsonMapper);
    final GranularitySpec granularitySpec = schema.getGranularitySpec();
    final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(parser.getDimensionsSpec())
        .withMetrics(schema.getAggregators())
        .withQueryGranularity(granularitySpec.getQueryGranularity())
        .withSegmentGranularity(granularitySpec.getSegmentGranularity())
        .withRollup(granularitySpec.isRollup())
        .withDimensionFixed(true)
        .withNoQuery(true)
        .build();

    final Map<String, Object> forwardContext = Maps.newHashMap(properties);
    forwardContext.put(FORMAT, INDEX_FORMAT);
    forwardContext.put(SCHEMA, jsonMapper.convertValue(indexSchema, ObjectMappers.MAP_REF));
    forwardContext.put(TUNING_CONFIG, jsonMapper.convertValue(tuningConfig, ObjectMappers.MAP_REF));
    forwardContext.put(TIMESTAMP_COLUMN, Row.TIME_COLUMN_NAME);
    forwardContext.put(DATASOURCE, schema.getDataSource());
    forwardContext.put(REGISTER_TABLE, true);
    forwardContext.put(TEMPORARY, isTemporary());
    forwardContext.put(OVERWRITE, isOverwrite());

    final DummyQuery<Row> query = DummyQuery.instance().withOverriddenContext(
        ImmutableMap.<String, Object>of(
            BaseQuery.QUERYID, UUID.randomUUID().toString(),
            Query.FORWARD_URL, ForwardConstants.LOCAL_TEMP_URL,
            Query.FORWARD_CONTEXT, forwardContext,
            QueryContextKeys.POST_PROCESSING, ImmutableMap.of("type", "rowToMap") // dummy to skip tabulating
        )
    );

    // use properties for encoding, extractPartition, etc.
    final Map<String, Object> loadContext = Maps.newHashMap(properties);
    loadContext.put(IGNORE_INVALID_ROWS, tuningConfig != null && tuningConfig.isIgnoreInvalidRows());
    loadContext.put(INPUT_FORMAT, inputFormat);

    // progressing sequence
    try {
      final Sequence<Row> sequence = Sequences.filterNull(handler.read(locations, parser, loadContext));
      return Pair.of(query, Sequences.map(sequence, Rows.rowToMap(Row.TIME_COLUMN_NAME)));
    }
    catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public String toString()
  {
    return "BrokerLoadSpec{" +
           "basePath=" + basePath +
           ", elements=" + paths +
           ", extension=" + extension +
           ", inputFormat=" + inputFormat +
           ", schema=" + schema +
           ", temporary=" + temporary +
           ", tuningConfig=" + tuningConfig +
           ", properties=" + properties +
           '}';
  }
}
