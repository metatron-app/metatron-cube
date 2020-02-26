package io.druid.java.util.emitter.service;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.emitter.core.Event;
import org.joda.time.DateTime;

import java.util.Map;

public class QueryEvent implements Event
{
  private final DateTime createdTime;
  private final String id;
  private final String remoteAddress;
  private final String query;
  private final String success;

  public QueryEvent(
      DateTime createdTime,
      String id,
      String remoteAddress,
      String query,
      String success
  )
  {
    this.createdTime = createdTime != null ? createdTime : new DateTime();
    this.id = id;
    this.remoteAddress = remoteAddress;
    this.query = query;
    this.success = success;
  }

  public String getId()
  {
    return id;
  }

  public String getRemoteAddress()
  {
    return remoteAddress;
  }

  public Object getQuery()
  {
    return query;
  }

  @Override
  public Map<String, Object> toMap()
  {
    return ImmutableMap.<String, Object>builder()
        .put("feed", getFeed())
        .put("timestamp", createdTime.toString())
        .put("id", id)
        .put("remoteAddress", Strings.nullToEmpty(remoteAddress))
        .put("query", query)
        .put("success", success)
        .build();
  }

  @Override
  public String getFeed()
  {
    return "queries";
  }

  @Override
  public DateTime getCreatedTime()
  {
    return createdTime;
  }

}
