package io.druid.server.log;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.inject.BindingAnnotation;
import com.metamx.emitter.core.Event;
import org.joda.time.DateTime;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.Map;

/**
 */
@Retention(RetentionPolicy.RUNTIME)
@BindingAnnotation
public @interface Events
{
  class SimpleEvent implements Event
  {
    private final Map<String, Object> event;

    public SimpleEvent(Map<String, Object> event)
    {
      this.event = event;
    }

    @Override
    @JsonValue
    public Map<String, Object> toMap()
    {
      return event;
    }

    @Override
    public String getFeed()
    {
      return (String) event.get("feed");
    }

    @Override
    public DateTime getCreatedTime()
    {
      return new DateTime(event.get("createdTime"));
    }

    @Override
    public boolean isSafeToBuffer()
    {
      return true;
    }
  }
}
