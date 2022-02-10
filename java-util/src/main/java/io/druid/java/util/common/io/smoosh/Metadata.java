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

package io.druid.java.util.common.io.smoosh;

import java.nio.ByteBuffer;

/**
 */
public class Metadata
{
  private final int fileNum;
  private final int startOffset;
  private final int endOffset;

  Metadata(int fileNum, int startOffset, int endOffset)
  {
    this.fileNum = fileNum;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  public int getFileNum()
  {
    return fileNum;
  }

  public int getStartOffset()
  {
    return startOffset;
  }

  public int getEndOffset()
  {
    return endOffset;
  }

  public int getLength()
  {
    return endOffset - startOffset;
  }

  public ByteBuffer slice(ByteBuffer buffer)
  {
    final ByteBuffer duplicate = buffer.duplicate();
    duplicate.position(startOffset).limit(endOffset);
    return duplicate.slice();
  }

  public Metadata shift(int offset)
  {
    final int delta = offset - startOffset;
    return delta == 0 ? this : new Metadata(fileNum, offset, endOffset + delta);
  }

  @Override
  public String toString()
  {
    return String.format("%d:%d~%d(%d)", fileNum, startOffset, endOffset, getLength());
  }
}
