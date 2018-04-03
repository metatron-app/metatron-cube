package io.druid.common.guava;

import java.io.File;

/**
 *
 */
public class Files
{
  // from com.google.common.io.Files.. except replaced mkdir to mkdirs
  private static final int TEMP_DIR_ATTEMPTS = 10000;

  public static File createTempDir()
  {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    String baseName = System.currentTimeMillis() + "-";

    for (int counter = 0; counter < TEMP_DIR_ATTEMPTS; counter++) {
      File tempDir = new File(baseDir, baseName + counter);
      if (tempDir.mkdirs()) {
        return tempDir;
      }
    }
    throw new IllegalStateException(
        "Failed to create directory within "
        + TEMP_DIR_ATTEMPTS + " attempts (tried "
        + baseName + "0 to " + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')'
    );
  }
}
