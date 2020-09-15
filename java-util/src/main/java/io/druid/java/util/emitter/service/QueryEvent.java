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
  private final Long timeNs;
  private final Long bytes;
  private final Integer rows;
  private String exception;
  private String interrupted;

  public QueryEvent(
      DateTime createdTime,
      String remoteAddress,
      String id,
      String query,
      String success,
      Long timeNs,
      Long bytes,
      Integer rows,
      String exception,
      String interrupted
  )
  {
    this.createdTime = createdTime != null ? createdTime : new DateTime();
    this.id = id;
    this.remoteAddress = remoteAddress;
    this.query = query;
    this.success = success;
    this.timeNs = timeNs;
    this.bytes = bytes;
    this.rows = rows;
    this.exception = exception;
    this.interrupted = interrupted;
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
  public String getFeed()
  {
    return "queries";
  }

  @Override
  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  public String getSuccess() {
    return success;
  }

  public Long getTimeNs() {
    return timeNs;
  }

  public Long getBytes() {
    return bytes;
  }

  public Integer getRows() {
    return rows;
  }

  public String getException() {
    return exception;
  }

  public String getInterrupted() {
    return interrupted;
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
            .put("querytime", timeNs)
            .put("bytes", bytes)
            .put("rows", rows)
            .put("exception", exception)
            .put("interrupted", interrupted)
            .build();
  }

}
