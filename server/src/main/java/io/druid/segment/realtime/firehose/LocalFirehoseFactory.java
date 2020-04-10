/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.druid.common.Progressing;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.InputRowParsers;
import io.druid.data.input.Rows;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.InputRowParser.Streaming;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.utils.Runnables;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 */
public class LocalFirehoseFactory implements FirehoseFactory
{
  private static final EmittingLogger log = new EmittingLogger(LocalFirehoseFactory.class);

  private final File baseDir;
  private final String filter;
  private final String encoding;
  private final boolean extractPartition;
  private final InputRowParser parser;

  @JsonCreator
  public LocalFirehoseFactory(
      @JsonProperty("baseDir") File baseDir,
      @JsonProperty("filter") String filter,
      @JsonProperty("encoding") String encoding,
      @JsonProperty("extractPartition") boolean extractPartition,
      // Backwards compatible
      @JsonProperty("parser") InputRowParser parser
  )
  {
    this.baseDir = Preconditions.checkNotNull(baseDir, "'baseDir' should not be null");
    this.filter = filter;
    this.encoding = encoding;
    this.extractPartition = extractPartition;
    this.parser = parser;
  }

  public LocalFirehoseFactory(File baseDir, String filter)
  {
    this(baseDir, filter, null, false, null);
  }

  @JsonProperty
  public File getBaseDir()
  {
    return baseDir;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFilter()
  {
    return filter;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getEncoding()
  {
    return encoding;
  }

  @JsonProperty
  public boolean isExtractPartition()
  {
    return extractPartition;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public InputRowParser getParser()
  {
    return parser;
  }

  @Override
  public Firehose connect(final InputRowParser parser) throws IOException
  {
    if (baseDir == null) {
      throw new IAE("baseDir is null");
    }
    log.info("Searching for all [%s] in and beneath [%s]", filter, baseDir.getAbsoluteFile());

    final Collection<File> files;
    if (baseDir.isFile()) {
      files = ImmutableList.of(baseDir);
    } else {
      files = FileUtils.listFiles(
          baseDir.getAbsoluteFile(),
          new WildcardFileFilter(filter),
          TrueFileFilter.INSTANCE
      );
    }
    if (files.isEmpty()) {
      throw new ISE("Found no files to ingest! Check your schema.");
    }
    log.info("Found files: " + files);

    final List<Pair<File, Integer>> foundFiles = GuavaUtils.zipWithIndex(files);
    final long[] lengths = new long[foundFiles.size()];
    for (int i = 0; i < foundFiles.size(); i++) {
      lengths[i] = (i > 0 ? lengths[i - 1] : 0) + foundFiles.get(i).lhs.length();
    }

    final Iterator<Iterator<InputRow>> readers = Iterators.transform(
        foundFiles.iterator(),
        new Function<Pair<File, Integer>, Iterator<InputRow>>()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Iterator<InputRow> apply(Pair<File, Integer> input)
          {
            final int index = input.rhs;
            try {
              final FileInputStream stream = new FileInputStream(input.lhs);
              final Reader reader = new InputStreamReader(stream, Charsets.toCharset(encoding));
              final Iterator<InputRow> iterator;
              if (parser instanceof Streaming && ((Streaming) parser).accept(reader)) {
                iterator = ((Streaming) parser).parseStream(reader);
              } else {
                iterator = Iterators.transform(IOUtils.lineIterator(reader), InputRowParsers.asFunction(parser, false));
              }
              if (extractPartition) {
                Rows.setPartition(input.lhs);
              }
              return new GuavaUtils.DelegatedProgressing<InputRow>(GuavaUtils.withResource(iterator, reader))
              {
                @Override
                public float progress()
                {
                  try {
                    return (lengths[index] - stream.available()) / (float) lengths[lengths.length - 1];
                  }
                  catch (IOException e) {
                    return lengths[index] / (float) lengths[lengths.length - 1];
                  }
                }
              };
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );

    return new Progressing.OnFirehose()
    {
      private Iterator<InputRow> current = Collections.emptyIterator();

      @Override
      public float progress()
      {
        return current instanceof Progressing ? ((Progressing) current).progress() : hasMore() ? 0 : 1;
      }

      @Override
      public void close() throws IOException
      {
        if (current instanceof Closeable) {
          IOUtils.closeQuietly((Closeable) current);
        }
      }

      @Override
      public boolean hasMore()
      {
        for (; !current.hasNext() && readers.hasNext(); current = readers.next()) {
        }
        return current.hasNext();
      }

      @Override
      @SuppressWarnings("unchecked")
      public InputRow nextRow()
      {
        return current.next();
      }

      @Override
      public Runnable commit()
      {
        return Runnables.getNoopRunnable();
      }
    };
  }
}
