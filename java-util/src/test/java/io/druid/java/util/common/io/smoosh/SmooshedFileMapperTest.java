/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. SK Telecom licenses this file
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

package io.druid.java.util.common.io.smoosh;

import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.BufferUtils;
import io.druid.java.util.common.ISE;
import junit.framework.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 */
public class SmooshedFileMapperTest
{
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  public SmooshedFileMapperTest() {
  }

  @Test
  public void testSanity() throws Exception {
    File baseDir = this.folder.newFolder("base");
    FileSmoosher smoosher = new FileSmoosher(baseDir, 21);
    Throwable var3 = null;

    try {
      for(int i = 0; i < 20; ++i) {
        File tmpFile = this.folder.newFile(String.format("smoosh-%s.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        smoosher.add(String.format("%d", i), tmpFile);
      }
    } catch (Throwable var29) {
      var3 = var29;
      throw var29;
    } finally {
      if (smoosher != null) {
        if (var3 != null) {
          try {
            smoosher.close();
          } catch (Throwable var26) {
            var3.addSuppressed(var26);
          }
        } else {
          smoosher.close();
        }
      }

    }

    File[] files = baseDir.listFiles();
    Assert.assertNotNull(files);
    Arrays.sort(files);
    Assert.assertEquals(5, files.length);

    for(int i = 0; i < 4; ++i) {
      Assert.assertEquals(FileSmoosher.makeChunkFile(baseDir, i), files[i]);
    }

    Assert.assertEquals(FileSmoosher.metaFile(baseDir), files[files.length - 1]);
    SmooshedFileMapper mapper = SmooshedFileMapper.load(baseDir);
    Throwable var34 = null;

    try {
      for(int i = 0; i < 20; ++i) {
        ByteBuffer buf = mapper.mapFile(String.format("%d", i));
        Assert.assertEquals(0, buf.position());
        Assert.assertEquals(4, buf.remaining());
        Assert.assertEquals(4, buf.capacity());
        Assert.assertEquals(i, buf.getInt());
      }
    } catch (Throwable var27) {
      var34 = var27;
      throw var27;
    } finally {
      if (mapper != null) {
        if (var34 != null) {
          try {
            mapper.close();
          } catch (Throwable var25) {
            var34.addSuppressed(var25);
          }
        } else {
          mapper.close();
        }
      }

    }

  }

  @Test
  public void testBehaviorWhenReportedSizesLargeAndExceptionIgnored() throws Exception {
    File baseDir = this.folder.newFolder("base");
    FileSmoosher smoosher = new FileSmoosher(baseDir, 21);
    Throwable var3 = null;

    try {
      for(int i = 0; i < 20; ++i) {
        SmooshedWriter writer = smoosher.addWithSmooshedWriter(String.format("%d", i), 7L);
        writer.write(ByteBuffer.wrap(Ints.toByteArray(i)));

        try {
          writer.close();
          Assert.fail("IOException expected");
        } catch (IOException var30) {
          ;
        }
      }
    } catch (Throwable var33) {
      var3 = var33;
      throw var33;
    } finally {
      if (smoosher != null) {
        if (var3 != null) {
          try {
            smoosher.close();
          } catch (Throwable var28) {
            var3.addSuppressed(var28);
          }
        } else {
          smoosher.close();
        }
      }

    }

    File[] files = baseDir.listFiles();
    Assert.assertNotNull(files);
    Arrays.sort(files);
    Assert.assertEquals(6, files.length);

    for(int i = 0; i < 4; ++i) {
      Assert.assertEquals(FileSmoosher.makeChunkFile(baseDir, i), files[i]);
    }

    Assert.assertEquals(FileSmoosher.metaFile(baseDir), files[files.length - 1]);
    SmooshedFileMapper mapper = SmooshedFileMapper.load(baseDir);
    Throwable var38 = null;

    try {
      for(int i = 0; i < 20; ++i) {
        ByteBuffer buf = mapper.mapFile(String.format("%d", i));
        Assert.assertEquals(0, buf.position());
        Assert.assertEquals(4, buf.remaining());
        Assert.assertEquals(4, buf.capacity());
        Assert.assertEquals(i, buf.getInt());
      }
    } catch (Throwable var31) {
      var38 = var31;
      throw var31;
    } finally {
      if (mapper != null) {
        if (var38 != null) {
          try {
            mapper.close();
          } catch (Throwable var29) {
            var38.addSuppressed(var29);
          }
        } else {
          mapper.close();
        }
      }

    }

  }

  @Test
  public void testBehaviorWhenReportedSizesSmall() throws Exception {
    File baseDir = this.folder.newFolder("base");
    FileSmoosher smoosher = new FileSmoosher(baseDir, 21);
    Throwable var3 = null;

    try {
      boolean exceptionThrown = false;

      try {
        SmooshedWriter writer = smoosher.addWithSmooshedWriter("1", 2L);
        Throwable var6 = null;

        try {
          writer.write(ByteBuffer.wrap(Ints.toByteArray(1)));
        } catch (Throwable var31) {
          var6 = var31;
          throw var31;
        } finally {
          if (writer != null) {
            if (var6 != null) {
              try {
                writer.close();
              } catch (Throwable var30) {
                var6.addSuppressed(var30);
              }
            } else {
              writer.close();
            }
          }

        }
      } catch (ISE var33) {
        Assert.assertTrue(var33.getMessage().contains("Liar!!!"));
        exceptionThrown = true;
      }

      Assert.assertTrue(exceptionThrown);
      File[] files = baseDir.listFiles();
      Assert.assertNotNull(files);
      Assert.assertEquals(1, files.length);
    } catch (Throwable var34) {
      var3 = var34;
      throw var34;
    } finally {
      if (smoosher != null) {
        if (var3 != null) {
          try {
            smoosher.close();
          } catch (Throwable var29) {
            var3.addSuppressed(var29);
          }
        } else {
          smoosher.close();
        }
      }

    }

  }

  @Test
  public void testDeterministicFileUnmapping() throws IOException {
    File baseDir = this.folder.newFolder("base");
    long totalMemoryUsedBeforeAddingFile = BufferUtils.totalMemoryUsedByDirectAndMappedBuffers();
    FileSmoosher smoosher = new FileSmoosher(baseDir);
    Throwable var5 = null;

    try {
      File dataFile = this.folder.newFile("data.bin");
      RandomAccessFile raf = new RandomAccessFile(dataFile, "rw");
      Throwable var8 = null;

      try {
        raf.setLength(1048576L);
      } catch (Throwable var31) {
        var8 = var31;
        throw var31;
      } finally {
        if (raf != null) {
          if (var8 != null) {
            try {
              raf.close();
            } catch (Throwable var30) {
              var8.addSuppressed(var30);
            }
          } else {
            raf.close();
          }
        }

      }

      smoosher.add(dataFile);
    } catch (Throwable var33) {
      var5 = var33;
      throw var33;
    } finally {
      if (smoosher != null) {
        if (var5 != null) {
          try {
            smoosher.close();
          } catch (Throwable var29) {
            var5.addSuppressed(var29);
          }
        } else {
          smoosher.close();
        }
      }

    }

    long totalMemoryUsedAfterAddingFile = BufferUtils.totalMemoryUsedByDirectAndMappedBuffers();
    Assert.assertEquals(totalMemoryUsedBeforeAddingFile, totalMemoryUsedAfterAddingFile);
  }
}
