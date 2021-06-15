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

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.segment.IndexSpec;
import io.druid.segment.incremental.BaseTuningConfig;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class AbstractResolver implements FileLoadSpec.Resolver
{
  protected final String basePath;  // optional absolute path (paths in elements are regarded as relative to this)
  protected final List<String> paths;
  protected final boolean recursive;

  protected final String timeExpression;
  protected final Granularity segmentGranularity;
  protected final Map<String, Object> properties = Maps.newHashMap();

  @JsonCreator
  public AbstractResolver(
      @JsonProperty("basePath") String basePath,
      @JsonProperty("paths") String paths,
      @JsonProperty("recursive") boolean recursive,
      @JsonProperty("timeExpression") String timeExpression,
      @JsonProperty("segmentGranularity") Granularity segmentGranularity
  )
  {
    Preconditions.checkArgument(basePath != null || !StringUtils.isNullOrEmpty(paths), "No path");
    this.basePath = Preconditions.checkNotNull(basePath);
    this.paths = paths == null ? null : Arrays.asList(paths.split(","));
    this.recursive = recursive;
    this.timeExpression = timeExpression;
    this.segmentGranularity = segmentGranularity;
  }

  @JsonAnySetter
  public void property(String name, Object value)
  {
    properties.put(name, value);
  }

  protected List<AggregatorFactory> rewriteMetrics(
      List<AggregatorFactory> metrics, Map<String, Object> properties, ObjectMapper mapper
  ) throws IOException
  {
    List<String> metricNames = AggregatorFactory.toNames(metrics);
    // extract.<column-name> = <typeName>,<extract-hint1>,<extract-hint2>,...
    Set<String> extractedNames = Sets.newHashSet();
    List<AggregatorFactory> appended = Lists.newArrayList();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      final String key = entry.getKey();
      if (key.startsWith("extract.") || key.startsWith("evaluate.")) {
        final String[] values = Objects.toString(entry.getValue()).split(",");
        final String columnName = key.substring(key.indexOf('.') + 1, key.length());
        final String columnType = values[0];
        final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(columnType);
        if (serde == null) {
          throw new IAE("cannot handle type [%s] for column [%s]", columnType, columnName);
        }
        final List<String> hints = Arrays.asList(values).subList(1, values.length);
        final ComplexMetricExtractor extractor = serde.getExtractor(hints);
        if (extractor == null) {
          throw new IAE("cannot find extractor for column [%s] with hints %s", columnName, hints);
        }
        if (key.startsWith("extract.")) {
          extractedNames.addAll(extractor.getExtractedNames(metricNames));
        }
        appended.add(new RelayAggregatorFactory(columnName, columnName, columnType, null, hints));
      }
    }
    if (!extractedNames.isEmpty()) {
      metrics = Lists.newArrayList(Iterables.filter(metrics, m -> !extractedNames.contains(m.getName())));
    }
    metrics.addAll(appended);
    return metrics;
  }

  protected BaseTuningConfig tuningConfigFromProperties(Map<String, Object> properties, ObjectMapper mapper)
  {
    Map<String, Object> tc = mappify(properties, "tunning.");
    BaseTuningConfig tunning = tc.isEmpty() ? null : mapper.convertValue(tc, BaseTuningConfig.class);

    Map<String, Object> ic = mappify(properties, "indexing.");
    if (!ic.isEmpty()) {
      IndexSpec indexSpec = mapper.convertValue(ic, IndexSpec.class);
      if (indexSpec != null && !IndexSpec.DEFAULT.equals(indexSpec)) {
        tunning = (tunning == null ? BaseTuningConfig.DEFAULT : tunning).withIndexSpec(indexSpec);
      }
    }
    return tunning;
  }

  protected Map<String, Object> mappify(Map<String, Object> properties, String prefix)
  {
    Map<String, Object> map = Maps.newHashMap();
    for (Map.Entry<String, Object> entry : Maps.filterKeys(properties, k -> k.startsWith(prefix)).entrySet()) {
      String name = entry.getKey();
      String key = name.substring(prefix.length(), name.length());
      int index = key.indexOf('.');
      if (index < 0) {
        map.put(key, entry.getValue());
      } else if (!map.containsKey(key.substring(0, index))) {
        map.put(key.substring(0, index), mappify(properties, prefix + key.substring(0, index + 1)));
      }
    }
    return map;
  }

  protected List<String> resolve(String basePath, List<String> paths, boolean recursive) throws IOException
  {
    if (GuavaUtils.isNullOrEmpty(paths)) {
      return resolvePath(toPath(basePath), recursive);
    }
    if (StringUtils.isNullOrEmpty(basePath)) {
      List<String> resolved = Lists.newArrayList();
      for (Path path : GuavaUtils.transform(paths, p -> toPath(p))) {
        resolved.addAll(resolvePath(path, recursive));
      }
      return resolved;
    } else {
      Path parent = toPath(basePath);
      if (!parent.isAbsolute() || getFirstGlopPath(parent) >= 0) {
        throw new IAE("'basePath' should be not-globbing absolute one but %s", parent);
      }
      List<String> resolved = Lists.newArrayList();
      for (Path path : GuavaUtils.transform(paths, p -> parent.resolve(toPath(p)))) {
        resolved.addAll(resolvePath(path, recursive));
      }
      return resolved;
    }
  }

  private static List<String> resolvePath(Path path, boolean recursive) throws IOException
  {
    if (!path.isAbsolute()) {
      throw new IAE("path should be absolute but %s", path);
    }
    int x = getFirstGlopPath(path);
    if (x < 0) {
      return ImmutableList.copyOf(resolve(path, recursive));
    }
    List<String> result = Lists.newArrayList();
    int nameCount = path.getNameCount();
    Path parent = x == 0 ? path.getRoot() : path.subpath(0, x - 1);
    try (DirectoryStream<Path> paths = Files.newDirectoryStream(parent, "glob:" + path.subpath(x, nameCount - 1))) {
      for (Path found : paths) {
        Iterables.addAll(result, resolve(found, recursive));
      }
    }
    return result;
  }

  private static Iterable<String> resolve(Path path, boolean recursive) throws IOException
  {
    if (Files.isDirectory(path)) {
      if (recursive) {
        List<String> result = Lists.newArrayList();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(path, f -> !Files.isDirectory(f))) {
          for (Path found : paths) {
            result.add(found.toString());
          }
        }
        return result;
      } else {
        return Iterables.transform(Arrays.asList(path.toFile().listFiles(f1 -> f1.isFile())), f -> f.getAbsolutePath());
      }
    } else {
      return Arrays.asList(path.toString());
    }
  }

  // sun.nio.fs.Globs
  private static final String regexMetaChars = ".^$+{[]|()";
  private static final String globMetaChars = "\\*?[{";

  private static int getFirstGlopPath(Path path)
  {
    int count = path.getNameCount();
    for (int i = 0; i < count; i++) {
      if (hasGlobPath(path.getName(i).toString())) {
        return i;
      }
    }
    return -1;
  }

  private static boolean hasGlobPath(String p)
  {
    return p.indexOf('*') >= 0 || p.indexOf('?') >= 0 || p.indexOf('[') >= 0 || p.indexOf('{') >= 0;
  }

  private static Path toPath(String basePath)
  {
    try {
      URI uri = new URI(basePath);
      if (uri.getScheme() != null) {
        // I don't know why this fuck is required
        return FileSystems.getFileSystem(new URI(uri.getScheme(), null, "/", null)).getPath(uri.getPath());
      }
      return FileSystems.getDefault().getPath(basePath);
    }
    catch (URISyntaxException e) {
      throw new IAE(e, "invalid path %s", basePath);
    }
  }
}
