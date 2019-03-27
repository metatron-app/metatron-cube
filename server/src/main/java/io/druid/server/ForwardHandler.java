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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.PropUtils;
import io.druid.data.output.Formatters;
import io.druid.guice.LocalDataStorageDruidModule;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.query.BaseQuery;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.StorageHandler;
import io.druid.query.TabularFormat;
import io.druid.segment.incremental.IncrementalIndexSchema;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;

public class ForwardHandler implements ForwardConstants
{
  protected static final Logger LOG = new Logger(ForwardHandler.class);

  protected final DruidNode node;
  protected final ObjectMapper jsonMapper;
  protected final QueryToolChestWarehouse warehouse;
  protected final Map<String, StorageHandler> handlerMap;
  protected final QuerySegmentWalker segmentWalker;

  @Inject
  public ForwardHandler(
      @Self DruidNode node,
      @Json ObjectMapper jsonMapper,
      QueryToolChestWarehouse warehouse,
      Map<String, StorageHandler> handlerMap,
      QuerySegmentWalker segmentWalker
  ) {
    this.node = node;
    this.jsonMapper = jsonMapper;
    this.warehouse = warehouse;
    this.handlerMap = handlerMap;
    this.segmentWalker = segmentWalker;
  }

  public StorageHandler getHandler(String scheme)
  {
    return scheme.equals("null") ? StorageHandler.NULL : handlerMap.get(scheme);
  }

  @SuppressWarnings("unchecked")
  public <T> QueryRunner<T> wrapForward(final Query<T> query, final QueryRunner<T> baseRunner)
  {
    final URI uri = getForwardURI(query);
    if (uri == null) {
      return baseRunner;
    }
    final String scheme = Optional.fromNullable(uri.getScheme()).or(StorageHandler.FILE_SCHEME);

    final StorageHandler handler = getHandler(scheme);
    if (handler == null) {
      LOG.warn("Unsupported scheme '" + scheme + "'");
      throw new IAE("Unsupported scheme '%s'", scheme);
    }
    final Map<String, Object> forwardContext = BaseQuery.getResultForwardContext(query);

    if (Formatters.isIndexFormat(forwardContext)) {
      Object indexSchema = forwardContext.get(SCHEMA);
      if (indexSchema == null) {
        IncrementalIndexSchema schema = Queries.relaySchema(query, segmentWalker).asRelaySchema();
        LOG.info(
            "Resolved index schema.. dimensions: %s, metrics: %s",
            schema.getDimensionsSpec().getDimensionNames(),
            Arrays.toString(schema.getMetrics())
        );
        indexSchema = schema;
      }
      forwardContext.put(
          SCHEMA,
          jsonMapper.convertValue(indexSchema, new TypeReference<Map<String, Object>>() { })
      );
      Object indexInterval = forwardContext.get(INTERVAL);
      if (indexInterval == null) {
        indexInterval = JodaUtils.umbrellaInterval(query.getIntervals());
      }
      forwardContext.put(INTERVAL, indexInterval.toString());
    }

    return new QueryRunner()
    {
      @Override
      public Sequence run(Query query, Map responseContext)
      {
        URI rewritten = uri;
        try {
          if (PropUtils.parseBoolean(forwardContext, Query.LOCAL_POST_PROCESSING)) {
            rewritten = rewriteURI(rewritten, scheme, null, rewritten.getPath() + "/" + node.toPathName());
          }
          if (scheme.equals(StorageHandler.FILE_SCHEME) || scheme.equals(LocalDataStorageDruidModule.SCHEME)) {
            rewritten = rewriteURI(rewritten, scheme, node, null);
          }
          if (Formatters.isIndexFormat(forwardContext) && "/__temporary".equals(rewritten.getPath())) {
            File output = GuavaUtils.createTemporaryDirectory("__druid_broker-", "-file_loader");
            rewritten = rewriteURI(rewritten, scheme, null, output.getAbsolutePath());
          }

          final TabularFormat input = toTabularFormat(removeForwardContext(query), responseContext);
          final Map<String, Object> result = handler.write(rewritten, input, forwardContext);
          return wrapForwardResult(query, forwardContext, result);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      private TabularFormat toTabularFormat(final Query query, final Map responseContext)
      {
        // union-all does not have toolchest. delegate it to inner query
        Query representative = BaseQuery.getRepresentative(query);
        if (PostProcessingOperators.isTabularOutput(query, jsonMapper)) {
          // already converted to tabular format
          return new TabularFormat()
          {
            @Override
            public Sequence getSequence() { return baseRunner.run(query, responseContext); }

            @Override
            public Map<String, Object> getMetaData() { return null; }
          };
        }
        String timestampColumn = PropUtils.parseString(forwardContext, Query.FORWARD_TIMESTAMP_COLUMN);
        return warehouse.getToolChest(representative).toTabularFormat(
            query, baseRunner.run(query, responseContext),
            timestampColumn
        );
      }
    };
  }

  // remove forward context (except select forward query) for historical, etc.
  private Query removeForwardContext(Query query)
  {
    return Queries.iterate(query, new Function<Query, Query>()
    {
      @Override
      public Query apply(Query input)
      {
        if (input.getContextValue(Query.FORWARD_URL) != null) {
          return input.withOverriddenContext(
              BaseQuery.contextRemover(Query.FORWARD_URL, Query.FORWARD_CONTEXT)
          );
        }
        return input;
      }
    });
  }

  private static URI getForwardURI(Query query)
  {
    String forwardURL = BaseQuery.getResultForwardURL(query);
    if (!Strings.isNullOrEmpty(forwardURL)) {
      try {
        return new URI(forwardURL);
      }
      catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
    }
    return null;
  }

  private static URI rewriteURI(URI uri, String scheme, DruidNode node, String path) throws URISyntaxException
  {
    return new URI(
        scheme,
        uri.getUserInfo(),
        node == null ? uri.getHost() : node.getHost(),
        node == null ? uri.getPort() : node.getPort(),
        path == null ? uri.getPath() : path,
        uri.getQuery(),
        uri.getFragment()
    );
  }

  protected Sequence wrapForwardResult(Query query, Map<String, Object> forwardContext, Map<String, Object> result)
      throws IOException
  {
    return Sequences.simple(Arrays.asList(result));
  }
}
