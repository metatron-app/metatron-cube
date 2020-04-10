//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package io.druid.common.guava;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;

import java.io.IOException;

public class HostAndPortDeserializer extends FromStringDeserializer<HostAndPort>
{
  private static final long serialVersionUID = 1L;
  public static final HostAndPortDeserializer std = new HostAndPortDeserializer();

  public HostAndPortDeserializer()
  {
    super(HostAndPort.class);
  }

  @Override
  public HostAndPort deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
  {
    if (jp.getCurrentToken() == JsonToken.START_OBJECT) {
      JsonNode root = jp.readValueAsTree();
      JsonNode hostNode = root.path("hostText");
      if (hostNode.isMissingNode()) {
        hostNode = root.path("host");
      }
      String host = hostNode.asText();
      JsonNode n = root.get("port");
      return n == null ? HostAndPort.fromString(host) : HostAndPort.fromParts(host, n.asInt());
    } else {
      return super.deserialize(jp, ctxt);
    }
  }

  @Override
  protected HostAndPort _deserialize(String value, DeserializationContext ctxt) throws IOException
  {
    return HostAndPort.fromString(value);
  }
}
