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
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
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
import io.druid.java.util.common.logger.Logger;
import io.druid.query.DummyQuery;
import io.druid.query.ForwardingSegmentWalker;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.RowToMap;
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
import java.util.Set;
import java.util.UUID;

/**
 */
public class FileLoadSpec implements ForwardConstants, ReadConstants
{
  private static final Logger LOG = new Logger(FileLoadSpec.class);

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format", defaultImpl = TypeStringResolver.class)
  public static interface Resolver
  {
    FileLoadSpec resolve(String dataSource, QuerySegmentWalker walker) throws IOException;
  }

  private final String basePath;  // optional absolute path (paths in elements are regarded as relative to this)

  private final List<String> paths;
  private final String inputFormat;
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

  public FileLoadSpec withExternalFormat(String extension, String inputFormat)
  {
    return new FileLoadSpec(
        basePath,
        paths,
        extension,
        inputFormat,
        schema,
        overwrite,
        temporary,
        tuningConfig,
        properties
    );
  }

  public FileLoadSpec augment(
      boolean overwrite,
      boolean temporary,
      Map<String, Object> properties
  )
  {
    return new FileLoadSpec(
        basePath,
        paths,
        extension,
        inputFormat,
        schema,
        overwrite,
        temporary,
        tuningConfig,
        properties
    );
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

  private List<URI> getURIs()
  {
    List<URI> uris = Lists.newArrayList();
    try {
      URI parent = basePath == null ? null : normalize(new URI(basePath));
      String prev = null;
      for (String path : paths) {
        URI child = new URI(path);
        if (child.isAbsolute()) {
          uris.add(child);
          continue;
        }
        URI resolved = resolve(parent, child);
        if (prev == null || prev.equals(resolved.getScheme())) {
          prev = resolved.getScheme();
          uris.add(resolved);
          continue;
        }
        throw new IAE("conflicting schema %s and %s", prev, resolved.getScheme());
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
    LOG.info("%s : %s", scheme, locations);
    final ObjectMapper jsonMapper = walker.getMapper();

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
            Query.QUERYID, UUID.randomUUID().toString(),
            Query.FORWARD_URL, ForwardConstants.LOCAL_TEMP_URL,
            Query.FORWARD_CONTEXT, forwardContext,
            Query.POST_PROCESSING, new RowToMap(Row.TIME_COLUMN_NAME)  // dummy to skip tabulating
        )
    );

    // use properties for encoding, extractPartition, etc.
    final Map<String, Object> loadContext = Maps.newHashMap(properties);
    loadContext.put(IGNORE_INVALID_ROWS, tuningConfig != null && tuningConfig.isIgnoreInvalidRows());
    loadContext.put(INPUT_FORMAT, inputFormat);
    final Set<String> requiredColumns = schema.getRequiredColumnNames(parser);
    if (requiredColumns != null && requiredColumns.size() > 0) {
      String concat = StringUtils.concat(",", requiredColumns);
      loadContext.put(DataSchema.REQUIRED_COLUMNS, concat);
      loadContext.put(DataSchema.HADOOP_REQUIRED_COLUMNS, concat);
    }

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
