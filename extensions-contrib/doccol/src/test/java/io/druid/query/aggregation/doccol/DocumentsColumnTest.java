package io.druid.query.aggregation.doccol;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class DocumentsColumnTest {
  public DocumentsColumnTest()
  {

  }

  @Test
  public void testToBytes() {
    try {
      String doc = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("three.txt"), "UTF-8");
      String doc2 = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("monte.txt"), "UTF-8");

      DocumentsColumn documentsColumn = new DocumentsColumn(true);
      documentsColumn.add(doc).add(doc2);
      DocumentsColumn documentsColumn2 = new DocumentsColumn(false);
      documentsColumn2.add(doc).add(doc2);

      byte[] bytes = documentsColumn.toBytes();
      DocumentsColumn ret = DocumentsColumn.fromBytes(bytes);

      Assert.assertEquals(documentsColumn, ret);

      byte[] bytes2 = documentsColumn2.toBytes();
      DocumentsColumn ret2 = DocumentsColumn.fromBytes(bytes2);

      Assert.assertEquals(documentsColumn, ret2);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
