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

package io.druid.server.http;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import com.metamx.common.Pair;
import com.metamx.common.guava.Comparators;
import com.metamx.common.logger.Logger;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.client.CoordinatorServerView;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.ImmutableSegmentLoadInfo;
import io.druid.client.SegmentLoadInfo;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.common.Intervals;
import io.druid.common.utils.JodaUtils;
import io.druid.metadata.ColumnDesc;
import io.druid.metadata.DescExtractor;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.metadata.TableDesc;
import io.druid.query.TableDataSource;
import io.druid.server.http.security.DatasourceResourceFilter;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineLookup;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
@Path("/druid/coordinator/v1/datasources")
public class DatasourcesResource
{
  private static final Logger log = new Logger(DatasourcesResource.class);

  private final CoordinatorServerView serverInventoryView;
  private final MetadataSegmentManager databaseSegmentManager;
  private final IndexingServiceClient indexingServiceClient;
  private final AuthConfig authConfig;

  @Inject
  public DatasourcesResource(
      CoordinatorServerView serverInventoryView,
      MetadataSegmentManager databaseSegmentManager,
      @Nullable IndexingServiceClient indexingServiceClient,
      AuthConfig authConfig
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.databaseSegmentManager = databaseSegmentManager;
    this.indexingServiceClient = indexingServiceClient;
    this.authConfig = authConfig;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getQueryableDataSources(
      @QueryParam("full") String full,
      @QueryParam("simple") String simple,
      @QueryParam("nameRegex") String regex,
      @Context final HttpServletRequest req
  )
  {
    Response.ResponseBuilder builder = Response.ok();
    Set<DruidDataSource> datasources = authConfig.isEnabled() ?
                                             InventoryViewUtils.getSecuredDataSources(
                                                 serverInventoryView,
                                                 (AuthorizationInfo) req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN)
                                             ) :
                                             InventoryViewUtils.getDataSources(serverInventoryView);
    if (!Strings.isNullOrEmpty(regex)) {
      Set<DruidDataSource> filtered = Sets.newLinkedHashSet();
      for (String part : regex.split(",")) {
        final Matcher matcher = Pattern.compile(part.trim()).matcher("");
        filtered.addAll(
            Sets.filter(
                datasources,
                new Predicate<DruidDataSource>()
                {
                  @Override
                  public boolean apply(DruidDataSource input)
                  {
                    return matcher.reset(input.getName()).matches();
                  }
                }
            )
        );
      }
      datasources = filtered;
    }

    if (full != null) {
      return builder.entity(datasources).build();
    } else if (simple != null) {
      return builder.entity(
          Lists.newArrayList(
              Iterables.transform(
                  datasources,
                  new Function<DruidDataSource, Map<String, Object>>()
                  {
                    @Override
                    public Map<String, Object> apply(DruidDataSource dataSource)
                    {
                      return makeSimpleDatasource(dataSource);
                    }
                  }
              )
          )
      ).build();
    }

    return builder.entity(
        Lists.newArrayList(
            Iterables.transform(
                datasources,
                new Function<DruidDataSource, String>()
                {
                  @Override
                  public String apply(DruidDataSource dataSource)
                  {
                    return dataSource.getName();
                  }
                }
            )
        )
    ).build();
  }

  @GET
  @Path("/{dataSourceName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getTheDataSource(
      @PathParam("dataSourceName") final String dataSourceName,
      @QueryParam("lastUpdated") final String lastUpdated,
      @QueryParam("full") final String full
  )
  {
    if (lastUpdated != null) {
      return getLastUpdatedSegment(dataSourceName);
    }
    DruidDataSource dataSource = getDataSource(dataSourceName);

    if (dataSource == null) {
      return Response.noContent().build();
    }

    if (full != null) {
      return Response.ok(dataSource).build();
    }

    return Response.ok(getSimpleDatasource(dataSourceName)).build();
  }

  @GET
  @Path("/{dataSourceName}/lastUpdate")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getLastUpdatedSegment(
      @PathParam("dataSourceName") final String dataSourceName
  )
  {
    Pair<String, DataSegment> lastUpdated = databaseSegmentManager.getLastUpdatedSegment(dataSourceName);
    if (lastUpdated == null) {
      return Response.noContent().build();
    }
    return Response.ok(ImmutableMap.of("lastUpdate", lastUpdated.lhs, "lastSegment", lastUpdated.rhs)).build();
  }

  @GET
  @Path("/{dataSourceName}/desc/{descType}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getTheDataSourceDesc(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("descType") String descType,
      @QueryParam("column") String columnName,
      @QueryParam("value") String columnValue)
  {
    log.info("getTheDataSourceDesc(%s, %s, %s)", dataSourceName, descType, columnName);
    TableDesc desc = databaseSegmentManager.getDataSourceDesc(dataSourceName);
    if (desc == null) {
      return Response.noContent().build();
    }
    DescExtractor type = DescExtractor.fromString(descType);
    switch (type) {
      case DS_COMMENT:
        // explicitly make json string
        return Response.ok("\"" + desc.getComment() + "\"").build();
      case DS_PROPS:
        return Response.ok(desc.getProperties()).build();
      case COLUMN_PROPS:
        Preconditions.checkArgument(columnName != null, "column name is missing");
        ColumnDesc entity = desc.getColumnDescs().get(columnName);
        if (entity == null) {
          return Response.noContent().build();
        }
        return Response.ok(entity.getProperties()).build();
      case COLUMNS_COMMENTS:
        Map<String, String> columns = Maps.newLinkedHashMap();
        for (Map.Entry<String, ColumnDesc> entry : desc.getColumnDescs().entrySet()) {
          columns.put(entry.getKey(), entry.getValue().getComment());
        }
        return Response.ok(columns).build();
      case VALUES_COMMENTS:
        Map<String, Map<String, String>> values = Maps.newLinkedHashMap();
        for (Map.Entry<String, ColumnDesc> entry : desc.getColumnDescs().entrySet()) {
          values.put(entry.getKey(), entry.getValue().getValueComments());
        }
        return Response.ok(values).build();
    }
    return Response.noContent().build();
  }

  @POST
  @Path("/{dataSourceName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response enableDataSource(
      @PathParam("dataSourceName") final String dataSourceName
  )
  {
    return databaseSegmentManager.enableDatasource(dataSourceName)
           ? Response.ok().build()
           : Response.noContent().build();

  }

  @POST
  @Path("/{dataSourceName}/disable")
  @ResourceFilters(DatasourceResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Response disableDataSource(
      @PathParam("dataSourceName") final String dataSourceName
  )
  {
    return databaseSegmentManager.disableDatasource(dataSourceName)
           ? Response.ok().build()
           : Response.noContent().build();
  }

  /* When this method is removed, a new method needs to be introduced corresponding to
    the end point "DELETE /druid/coordinator/v1/datasources/{dataSourceName}" (with no query parameters).
    Ultimately we want to have no method with kill parameter -
    DELETE `{dataSourceName}` will be used to disable datasource and
    DELETE `{dataSourceName}/intervals/{interval}` will be used to nuke segments
  */
  @DELETE
  @Deprecated
  @Path("/{dataSourceName}")
  @ResourceFilters(DatasourceResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteDataSource(
      @PathParam("dataSourceName") final String dataSourceName,
      @QueryParam("kill") final String kill,
      @QueryParam("interval") final String interval
  )
  {
    if (indexingServiceClient == null) {
      return Response.ok(ImmutableMap.of("error", "no indexing service found")).build();
    }

    if (Boolean.valueOf(kill)) {
      try {
        indexingServiceClient.killSegments(dataSourceName, Intervals.of(interval));
      }
      catch (Exception e) {
        return toDeleteExceptionResponse(e);
      }
      return Response.ok().build();
    } else {
      if (!databaseSegmentManager.disableDatasource(dataSourceName)) {
        return Response.noContent().build();
      }
    }

    return Response.ok().build();
  }

  @DELETE
  @Path("/{dataSourceName}/purge")
  @ResourceFilters(DatasourceResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Response purgeDataSource(
      @PathParam("dataSourceName") final String dataSourceName
  )
  {
    // disable + delete all
    if (indexingServiceClient == null) {
      return Response.ok(ImmutableMap.of("error", "no indexing service found")).build();
    }
    final Interval interval = databaseSegmentManager.getUmbrellaInterval(dataSourceName);
    if (interval == null) {
      return Response.noContent().build();
    }
    databaseSegmentManager.disableDatasource(dataSourceName);
    try {
      indexingServiceClient.killSegments(dataSourceName, interval);
    } catch (Exception e) {
      return toDeleteExceptionResponse(e);
    }
    return Response.ok().build();
  }

  @DELETE
  @Path("/{dataSourceName}/intervals/{interval}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteDataSourceSpecificInterval(
      @PathParam("dataSourceName") final String dataSourceName,
      @PathParam("interval") final String interval
  )
  {
    if (indexingServiceClient == null) {
      return Response.ok(ImmutableMap.of("error", "no indexing service found")).build();
    }
    final Interval theInterval = new Interval(interval.replace("_", "/"));
    try {
      indexingServiceClient.killSegments(dataSourceName, new Interval(theInterval));
    }
    catch (Exception e) {
      return toDeleteExceptionResponse(e);
    }
    return Response.ok().build();
  }

  private Response toDeleteExceptionResponse(Exception e)
  {
    if (e instanceof IllegalArgumentException) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(
                         ImmutableMap.of(
                             "error", "Exception occurred. Probably the interval is invalid",
                             "message", e.toString()
                         )
                     )
                     .build();
    }
    return Response.serverError()
                   .entity(
                       ImmutableMap.of(
                           "error", "Exception occurred. Are you sure you have an indexing service?",
                           "message", e.toString()
                       )
                   )
                   .build();
  }

  @GET
  @Path("/{dataSourceName}/intervals")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getSegmentDataSourceIntervals(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("simple") String simple,
      @QueryParam("full") String full
  )
  {
    final DruidDataSource dataSource = getDataSource(dataSourceName);

    if (dataSource == null) {
      return Response.noContent().build();
    }

    final Comparator<Interval> comparator = Comparators.inverse(JodaUtils.intervalsByStartThenEnd());

    if (full != null) {
      final Map<Interval, Map<String, Object>> retVal = Maps.newTreeMap(comparator);
      for (DataSegment dataSegment : dataSource.getSegments()) {
        Map<String, Object> segments = retVal.get(dataSegment.getInterval());
        if (segments == null) {
          segments = Maps.newHashMap();
          retVal.put(dataSegment.getInterval(), segments);
        }

        Pair<DataSegment, Set<String>> val = getSegment(dataSegment.getIdentifier());
        segments.put(dataSegment.getIdentifier(), ImmutableMap.of("metadata", val.lhs, "servers", val.rhs));
      }

      return Response.ok(retVal).build();
    }

    if (simple != null) {
      final Map<Interval, Map<String, Object>> retVal = Maps.newTreeMap(comparator);
      for (DataSegment dataSegment : dataSource.getSegments()) {
        Map<String, Object> properties = retVal.get(dataSegment.getInterval());
        if (properties == null) {
          properties = Maps.newHashMap();
          properties.put("size", dataSegment.getSize());
          properties.put("count", 1);

          retVal.put(dataSegment.getInterval(), properties);
        } else {
          properties.put("size", MapUtils.getLong(properties, "size", 0L) + dataSegment.getSize());
          properties.put("count", MapUtils.getInt(properties, "count", 0) + 1);
        }
      }

      return Response.ok(retVal).build();
    }

    final Set<Interval> intervals = Sets.newTreeSet(comparator);
    for (DataSegment dataSegment : dataSource.getSegments()) {
      intervals.add(dataSegment.getInterval());
    }

    return Response.ok(intervals).build();
  }

  @GET
  @Path("/{dataSourceName}/intervals/{interval}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getSegmentDataSourceSpecificInterval(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("interval") String interval,
      @QueryParam("simple") String simple,
      @QueryParam("full") String full
  )
  {
    final DruidDataSource dataSource = getDataSource(dataSourceName);
    final Interval theInterval = new Interval(interval.replace("_", "/"));

    if (dataSource == null) {
      return Response.noContent().build();
    }

    final Comparator<Interval> comparator = Comparators.inverse(JodaUtils.intervalsByStartThenEnd());
    if (full != null) {
      final Map<Interval, Map<String, Object>> retVal = Maps.newTreeMap(comparator);
      for (DataSegment dataSegment : dataSource.getSegments()) {
        if (theInterval.contains(dataSegment.getInterval())) {
          Map<String, Object> segments = retVal.get(dataSegment.getInterval());
          if (segments == null) {
            segments = Maps.newHashMap();
            retVal.put(dataSegment.getInterval(), segments);
          }

          Pair<DataSegment, Set<String>> val = getSegment(dataSegment.getIdentifier());
          segments.put(dataSegment.getIdentifier(), ImmutableMap.of("metadata", val.lhs, "servers", val.rhs));
        }
      }

      return Response.ok(retVal).build();
    }

    if (simple != null) {
      final Map<Interval, Map<String, Object>> retVal = Maps.newHashMap();
      for (DataSegment dataSegment : dataSource.getSegments()) {
        if (theInterval.contains(dataSegment.getInterval())) {
          Map<String, Object> properties = retVal.get(dataSegment.getInterval());
          if (properties == null) {
            properties = Maps.newHashMap();
            properties.put("size", dataSegment.getSize());
            properties.put("count", 1);

            retVal.put(dataSegment.getInterval(), properties);
          } else {
            properties.put("size", MapUtils.getLong(properties, "size", 0L) + dataSegment.getSize());
            properties.put("count", MapUtils.getInt(properties, "count", 0) + 1);
          }
        }
      }

      return Response.ok(retVal).build();
    }

    final Set<String> retVal = Sets.newTreeSet(Comparators.inverse(String.CASE_INSENSITIVE_ORDER));
    for (DataSegment dataSegment : dataSource.getSegments()) {
      if (theInterval.contains(dataSegment.getInterval())) {
        retVal.add(dataSegment.getIdentifier());
      }
    }

    return Response.ok(retVal).build();
  }

  @GET
  @Path("/{dataSourceName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getSegmentDataSourceSegments(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("full") String full
  )
  {
    DruidDataSource dataSource = getDataSource(dataSourceName);
    if (dataSource == null) {
      return Response.noContent().build();
    }

    Response.ResponseBuilder builder = Response.ok();
    if (full != null) {
      return builder.entity(dataSource.getSegmentsSorted()).build();
    }

    return builder.entity(
        Iterables.transform(
            dataSource.getSegmentsSorted(),
            new Function<DataSegment, Object>()
            {
              @Override
              public Object apply(DataSegment segment)
              {
                return segment.getIdentifier();
              }
            }
        )
    ).build();
  }

  @GET
  @Path("/{dataSourceName}/segments/{segmentId}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getSegmentDataSourceSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    DruidDataSource dataSource = getDataSource(dataSourceName);
    if (dataSource == null) {
      return Response.noContent().build();
    }

    Pair<DataSegment, Set<String>> retVal = getSegment(segmentId);

    if (retVal != null) {
      return Response.ok(
          ImmutableMap.of("metadata", retVal.lhs, "servers", retVal.rhs)
      ).build();
    }

    return Response.noContent().build();
  }

  @DELETE
  @Path("/{dataSourceName}/segments/{segmentId}")
  @ResourceFilters(DatasourceResourceFilter.class)
  @Deprecated
  public Response deleteDatasourceSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    if (!databaseSegmentManager.disableSegment(dataSourceName, segmentId)) {
      return Response.noContent().build();
    }

    return Response.ok().build();
  }

  @POST
  @Path("/{dataSourceName}/segment/disable/{segmentId}")
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response disableDatasourceSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    if (!databaseSegmentManager.disableSegment(dataSourceName, segmentId)) {
      return Response.noContent().build();
    }

    return Response.ok().build();
  }

  @POST
  @Path("/{dataSourceName}/interval/disable/{interval}")
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response disableDatasourceInterval(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("interval") String interval
  )
  {
    final Interval theInterval = new Interval(interval.replace("_", "/"));
    final int disabled = databaseSegmentManager.disableSegments(dataSourceName, theInterval);
    if (disabled < 0) {
      return Response.serverError().build();
    }

    return Response.ok(disabled).build();
  }

  @POST
  @Path("/{dataSourceName}/segments/{segmentId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response enableDatasourceSegment(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("segmentId") String segmentId
  )
  {
    if (!databaseSegmentManager.enableSegment(segmentId)) {
      return Response.noContent().build();
    }

    return Response.ok().build();
  }

  @GET
  @Path("/{dataSourceName}/tiers")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getSegmentDataSourceTiers(
      @PathParam("dataSourceName") String dataSourceName
  )
  {
    Set<String> retVal = Sets.newHashSet();
    for (DruidServer druidServer : serverInventoryView.getInventory()) {
      if (druidServer.getDataSource(dataSourceName) != null) {
        retVal.add(druidServer.getTier());
      }
    }

    return Response.ok(retVal).build();
  }

  private DruidDataSource getDataSource(final String dataSourceName)
  {
    Iterable<DruidDataSource> dataSources =
        Iterables.concat(
            Iterables.transform(
                serverInventoryView.getInventory(),
                new Function<DruidServer, DruidDataSource>()
                {
                  @Override
                  public DruidDataSource apply(DruidServer input)
                  {
                    return input.getDataSource(dataSourceName);
                  }
                }
            )
        );

    List<DruidDataSource> validDataSources = Lists.newArrayList();
    for (DruidDataSource dataSource : dataSources) {
      if (dataSource != null) {
        validDataSources.add(dataSource);
      }
    }
    if (validDataSources.isEmpty()) {
      return null;
    }

    Map<String, DataSegment> segmentMap = Maps.newHashMap();
    for (DruidDataSource dataSource : validDataSources) {
      if (dataSource != null) {
        Iterable<DataSegment> segments = dataSource.getSegments();
        for (DataSegment segment : segments) {
          segmentMap.put(segment.getIdentifier(), segment);
        }
      }
    }

    return new DruidDataSource(
        dataSourceName,
        ImmutableMap.<String, String>of()
    ).addSegments(segmentMap);
  }

  private Pair<DataSegment, Set<String>> getSegment(String segmentId)
  {
    DataSegment theSegment = null;
    Set<String> servers = Sets.newHashSet();
    for (DruidServer druidServer : serverInventoryView.getInventory()) {
      DataSegment currSegment = druidServer.getSegments().get(segmentId);
      if (currSegment != null) {
        theSegment = currSegment;
        servers.add(druidServer.getHost());
      }
    }

    if (theSegment == null) {
      return null;
    }

    return new Pair<>(theSegment, servers);
  }

  private Map<String, Object> makeSimpleDatasource(DruidDataSource input)
  {
    return new ImmutableMap.Builder<String, Object>()
        .put("name", input.getName())
        .put("properties", getSimpleDatasource(input.getName()))
        .build();
  }

  private Map<String, Map<String, Object>> getSimpleDatasource(String dataSourceName)
  {
    Map<String, Object> tiers = Maps.newHashMap();
    Map<String, Object> segments = Maps.newHashMap();
    Map<String, Map<String, Object>> retVal = ImmutableMap.of(
        "tiers", tiers,
        "segments", segments
    );
    Set<String> totalDistinctSegments = Sets.newHashSet();
    Map<String, HashSet<Object>> tierDistinctSegments = Maps.newHashMap();

    long totalSegmentSize = 0;
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    String tier;
    for (DruidServer druidServer : serverInventoryView.getInventory()) {
      DruidDataSource druidDataSource = druidServer.getDataSource(dataSourceName);
      tier = druidServer.getTier();

      if (druidDataSource == null) {
        continue;
      }

      if (!tierDistinctSegments.containsKey(tier)) {
        tierDistinctSegments.put(tier, Sets.newHashSet());
      }

      long dataSourceSegmentSize = 0;
      for (DataSegment dataSegment : druidDataSource.getSegments()) {
        // tier segments stats
        if (!tierDistinctSegments.get(tier).contains(dataSegment.getIdentifier())) {
          dataSourceSegmentSize += dataSegment.getSize();
          tierDistinctSegments.get(tier).add(dataSegment.getIdentifier());
        }
        // total segments stats
        if (!totalDistinctSegments.contains(dataSegment.getIdentifier())) {
          totalSegmentSize += dataSegment.getSize();
          totalDistinctSegments.add(dataSegment.getIdentifier());

          if (dataSegment.getInterval().getStartMillis() < minTime) {
            minTime = dataSegment.getInterval().getStartMillis();
          }
          if (dataSegment.getInterval().getEndMillis() > maxTime) {
            maxTime = dataSegment.getInterval().getEndMillis();
          }
        }
      }

      // tier stats
      Map<String, Object> tierStats = (Map) tiers.get(tier);
      if (tierStats == null) {
        tierStats = Maps.newHashMap();
        tiers.put(druidServer.getTier(), tierStats);
      }
      tierStats.put("segmentCount", tierDistinctSegments.get(tier).size());

      long segmentSize = MapUtils.getLong(tierStats, "size", 0L);
      tierStats.put("size", segmentSize + dataSourceSegmentSize);
    }

    segments.put("count", totalDistinctSegments.size());
    segments.put("size", totalSegmentSize);
    segments.put("minTime", new DateTime(minTime));
    segments.put("maxTime", new DateTime(maxTime));
    return retVal;
  }

  /**
   * Provides serverView for a datasource and Interval which gives details about servers hosting segments for an interval
   * Used by the realtime tasks to fetch a view of the interval they are interested in.
   */
  @GET
  @Path("/{dataSourceName}/intervals/{interval}/serverview")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getSegmentDataSourceSpecificInterval(
      @PathParam("dataSourceName") String dataSourceName,
      @PathParam("interval") String interval,
      @QueryParam("partial") final boolean partial
  )
  {
    TimelineLookup<String, SegmentLoadInfo> timeline = serverInventoryView.getTimeline(
        new TableDataSource(dataSourceName)
    );
    final Interval theInterval = new Interval(interval.replace("_", "/"));
    if (timeline == null) {
      log.debug("No timeline found for datasource[%s]", dataSourceName);
      return Response.ok(Lists.<ImmutableSegmentLoadInfo>newArrayList()).build();
    }

    final List<ImmutableSegmentLoadInfo> segments = Lists.newArrayList();
    for (TimelineObjectHolder<String, SegmentLoadInfo> lookup : timeline.lookupWithIncompletePartitions(theInterval)) {
      for (PartitionChunk<SegmentLoadInfo> segment : lookup.getObject()) {
        segments.add(segment.getObject().toImmutableSegmentLoadInfo());
      }
    }
    return Response.ok(segments).build();
  }
}
