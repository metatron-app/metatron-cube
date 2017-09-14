package io.druid.query.aggregation.doccol;

import com.fasterxml.jackson.annotation.JsonValue;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class DocumentsColumn {
  int numDoc = 0;
  final boolean compress;
  final static byte COMPRESS_FLAG = (byte)1;

  private final List<String> docs;

  public DocumentsColumn(
      boolean compress,
      String doc
  )
  {
    this(compress);
    add(doc);
  }

  public DocumentsColumn(
      boolean compress
  )
  {
    this.compress = compress;
    docs = new ArrayList<>();
  }

  public synchronized DocumentsColumn add(String doc)
  {
    docs.add(doc);
    numDoc++;
    return this;
  }

  public synchronized DocumentsColumn add(DocumentsColumn dc)
  {
    for(String doc: dc.get())
    {
      add(doc);
    }
    return this;
  }

  public List<String> get()
  {
    return docs;
  }

  public static int sizeMeta()
  {
    // numDoc + compress_flag
    return 4 + 1;
  }

  private static byte[] compress(String text)
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      OutputStream out = new DeflaterOutputStream(baos);
      out.write(text.getBytes("UTF-8"));
      out.close();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    return baos.toByteArray();
  }

  public static String decompress(byte[] bytes) {
    InputStream in = new InflaterInputStream(new ByteArrayInputStream(bytes));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      byte[] buffer = new byte[8192];
      int len;
      while((len = in.read(buffer))>0)
        baos.write(buffer, 0, len);
      return new String(baos.toByteArray(), "UTF-8");
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  @JsonValue
  public byte[] toBytes()
  {
    LinkedList<byte[]> bytes = new LinkedList<>();
    int totalSize = 0;

    for (String doc : docs) {
      try {
        byte[] docByte = compress ? compress(doc) : doc.getBytes("UTF-8");
        totalSize += docByte.length;
        bytes.add(docByte);
      } catch (IOException e) {
        throw new AssertionError(e);
      }
    }

    ByteBuffer buffer = ByteBuffer.allocate(4+1+4*numDoc+totalSize);
    buffer.putInt(numDoc);
    buffer.put(compress ? COMPRESS_FLAG : 0);
    for (byte[] docByte: bytes) {
      buffer.putInt(docByte.length).put(docByte);
    }

    return buffer.array();
  }

  public static DocumentsColumn fromBytes(ByteBuffer buffer)
  {
    int numDoc = buffer.getInt();
    byte flag = buffer.get();
    DocumentsColumn documentsColumn = new DocumentsColumn(flag == COMPRESS_FLAG);

    for (int idx = 0; idx < numDoc; idx++)
    {
      int size = buffer.getInt();
      byte[] docByte = new byte[size];
      buffer.get(docByte, 0, size);
      try {
        String doc = flag == COMPRESS_FLAG ? decompress(docByte) : new String(docByte, "UTF-8");
        documentsColumn.add(doc);
      } catch (IOException e) {
        throw new AssertionError(e);
      }
    }

    return documentsColumn;
  }

  public static DocumentsColumn fromBytes(byte[] bytes)
  {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    return fromBytes(buffer);
  }

  @Override
  public boolean equals(Object o)
  {
    DocumentsColumn other = (DocumentsColumn)o;

    if (numDoc != other.numDoc)
    {
      return false;
    }

    Iterator<String> thisItr = docs.iterator();
    Iterator<String> otherItr = other.docs.iterator();
    for (int idx = 0; idx < numDoc; idx++)
    {
      String thisStr = thisItr.next();
      String otherStr = otherItr.next();

      if (!thisStr.equals(otherStr))
      {
        return false;
      }
    }

    return true;
  }
}
