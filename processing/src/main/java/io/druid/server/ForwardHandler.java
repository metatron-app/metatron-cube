/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.PropUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.output.Formatters;
import io.druid.data.output.ForwardConstants;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.jackson.ObjectMappers;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseQuery;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryException;
import io.druid.query.QueryResult;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryUtils;
import io.druid.query.QueryVisitor;
import io.druid.query.StorageHandler;
import io.druid.segment.incremental.IncrementalIndexSchema;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

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

  public Map<String, StorageHandler> getHandlerMap()
  {
    return handlerMap;
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
      LOG.warn("Unsupported scheme '%s'", scheme);
      throw new IAE("Unsupported scheme '%s'", scheme);
    }
    final Map<String, Object> forwardContext = BaseQuery.getResultForwardContext(query);

    if (Formatters.isIndexFormat(forwardContext)) {
      Object indexSchema = forwardContext.get(SCHEMA);
      if (indexSchema == null) {
        IncrementalIndexSchema schema = IncrementalIndexSchema.from(Queries.relaySchema(query, segmentWalker));
        LOG.info(
            "Resolved index schema.. dimensions: %s, metrics: %s",
            schema.getDimensionsSpec().getDimensionNames(),
            Arrays.toString(schema.getMetrics())
        );
        indexSchema = schema;
      }
      forwardContext.put(SCHEMA, jsonMapper.convertValue(indexSchema, ObjectMappers.MAP_REF));
    }

    return new QueryRunner()
    {
      @Override
      public Sequence run(final Query query, final Map responseContext)
      {
        URI rewritten = uri;
        try {
          final Map<String, Object> context = prepareContext(query, forwardContext);
          if (PropUtils.parseBoolean(context, ForwardConstants.REWRITE_URI_WITH_LOCAL)) {
            rewritten = rewriteURI(rewritten, scheme, null, rewritten.getPath() + "/" + node.toPathName());
          }
          if (StorageHandler.FILE_SCHEME.equals(scheme) || StorageHandler.LOCAL_SCHEME.equals(scheme)) {
            rewritten = rewriteURI(rewritten, scheme, node, null);
          }
          if (LOCAL_TEMP_PATH.equals(rewritten.getPath())) {
            File output = GuavaUtils.createTemporaryDirectory("__druid_broker-", "-file_loader");
            rewritten = rewriteURI(rewritten, scheme, null, output.getAbsolutePath());
          }
          final String[] inputColumns = Formatters.parseStrings(context.get(COLUMNS));
          final String schema = Objects.toString(context.get(TYPE_STRING), null);
          final Sequence<Map<String, Object>> sequence = asMap(removeForwardContext(query), context, responseContext);
          final Supplier<String> typeString = Suppliers.memoize(new Supplier<String>()
          {
            @Override
            public String get()
            {
              return schema != null ? schema :
                     QueryUtils.retrieveSchema(query, segmentWalker).relay(query, true).asTypeString();
            }
          });
          return wrapForwardResult(
              query,
              context,
              handler.write(rewritten, new QueryResult(sequence, inputColumns, typeString), context)
          );
        }
        catch (Exception e) {
          throw QueryException.wrapIfNeeded(e);
        }
      }

      private Sequence<Map<String, Object>> asMap(Query<T> query, Map<String, Object> context, Map responseContext)
      {
        Sequence sequence = baseRunner.run(query, responseContext);
        if (PostProcessingOperators.returns(query) == Map.class) {
          // already converted to map
          return sequence;
        }
        if (query instanceof Query.MapOutputSupport) {
          return ((Query.MapOutputSupport) query).asMap(sequence);
        }
        // union-all does not have toolchest. delegate it to inner query
        Query<T> representative = BaseQuery.getRepresentative(query);
        QueryToolChest<T> toolChest = warehouse.getToolChest(representative);
        String timestampColumn = PropUtils.parseString(context, ForwardConstants.TIMESTAMP_COLUMN);
        return toolChest.asMap(query, timestampColumn).apply(sequence);
      }
    };
  }

  // remove forward context (except select forward query) for historical, etc.
  private Query removeForwardContext(Query query)
  {
    return Queries.iterate(query, new QueryVisitor()
    {
      @Override
      public Query out(Query input)
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

  private static URI getForwardURI(Query<?> query)
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

  protected Map<String, Object> prepareContext(Query query, Map<String, Object> context)
  {
    return context;
  }

  protected Sequence wrapForwardResult(Query query, Map<String, Object> forwardContext, Map<String, Object> result)
      throws IOException
  {
    return Sequences.simple(Arrays.asList(result));
  }
}
