package io.druid.data.input;

import io.druid.common.utils.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class BytesOutputStreamTest
{
  @Test
  public void testVarSizeString()
  {
    String[] tests = new String[6];
    tests[0] = "navis";
    for (int i = 1 ; i < tests.length; i++) {
      StringBuilder b = new StringBuilder();
      for (int j = 0; j < 16; j++) {
        b.append(tests[i - 1]);
      }
      tests[i] = b.toString();
    }
    BytesOutputStream out = new BytesOutputStream();
    for (int i = 0; i < tests.length; i++) {
      out.writeVarSizeBytes(StringUtils.toUtf8(tests[i]));
    }
    BytesInputStream in = new BytesInputStream(out.toByteArray());
    for (int i = 0; i < tests.length; i++) {
      Assert.assertEquals(tests[i], in.readVarSizeUTF());
    }
  }
}
