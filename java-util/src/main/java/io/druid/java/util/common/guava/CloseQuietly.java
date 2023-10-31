/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.common.guava;

import io.druid.java.util.common.logger.Logger;

import java.io.Closeable;

/**
 *
 */
public class CloseQuietly
{
  private static final Logger log = new Logger(CloseQuietly.class);

  public static void close(Object object)
  {
    if (object instanceof Closeable) {
      close((Closeable) object);
    } else if (object instanceof AutoCloseable) {
      close((AutoCloseable) object);
    }
  }

  public static void close(Closeable closeable)
  {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    }
    catch (Exception e) {
      log.error(e, "Exception thrown while closing %s", closeable);
    }
  }

  public static void close(AutoCloseable closeable)
  {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    }
    catch (Exception e) {
      log.error(e, "Exception thrown while closing %s", closeable);
    }
  }
}
