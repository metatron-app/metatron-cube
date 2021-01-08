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
package io.airlift.compress.zstd;

import io.airlift.compress.Compressor;
import io.druid.segment.data.CompressedObjectStrategy;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.airlift.compress.zstd.Constants.MAX_BLOCK_SIZE;
import static io.airlift.compress.zstd.UnsafeUtil.getAddress;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class ZstdCompressor
        implements Compressor, CompressedObjectStrategy.Compressor
{
    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        int result = uncompressedSize + (uncompressedSize >>> 8);

        if (uncompressedSize < MAX_BLOCK_SIZE) {
            result += (MAX_BLOCK_SIZE - uncompressedSize) >>> 10;
        }

        return result;
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        long inputAddress = ARRAY_BYTE_BASE_OFFSET + inputOffset;
        long outputAddress = ARRAY_BYTE_BASE_OFFSET + outputOffset;

        return ZstdFrameCompressor.compress(input, inputAddress, inputAddress + inputLength, output, outputAddress, outputAddress + maxOutputLength, CompressionParameters.DEFAULT_COMPRESSION_LEVEL);
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        Object inputBase;
        long inputAddress;
        long inputLimit;
        if (input.isDirect()) {
            inputBase = null;
            long address = getAddress(input);
            inputAddress = address + input.position();
            inputLimit = address + input.limit();
        }
        else if (input.hasArray()) {
            inputBase = input.array();
            inputAddress = ARRAY_BYTE_BASE_OFFSET + input.arrayOffset() + input.position();
            inputLimit = ARRAY_BYTE_BASE_OFFSET + input.arrayOffset() + input.limit();
        }
        else {
            inputBase = UnsafeUtil.getArray(input);
            int arrayOffset = UnsafeUtil.getArrayOffset(input);
            inputAddress = ARRAY_BYTE_BASE_OFFSET + arrayOffset + input.position();
            inputLimit = ARRAY_BYTE_BASE_OFFSET + arrayOffset + input.limit();
        }

        Object outputBase;
        long outputAddress;
        long outputLimit;
        if (output.isDirect()) {
            outputBase = null;
            long address = getAddress(output);
            outputAddress = address + output.position();
            outputLimit = address + output.limit();
        }
        else if (output.hasArray()) {
            outputBase = output.array();
            outputAddress = ARRAY_BYTE_BASE_OFFSET + output.arrayOffset() + output.position();
            outputLimit = ARRAY_BYTE_BASE_OFFSET + output.arrayOffset() + output.limit();
        }
        else {
            throw new IllegalArgumentException("Unsupported output ByteBuffer implementation " + output.getClass().getName());
        }

        // HACK: Assure JVM does not collect Slice wrappers while compressing, since the
        // collection may trigger freeing of the underlying memory resulting in a segfault
        // There is no other known way to signal to the JVM that an object should not be
        // collected in a block, and technically, the JVM is allowed to eliminate these locks.
        synchronized (input) {
            synchronized (output) {
                int written = ZstdFrameCompressor.compress(
                        inputBase,
                        inputAddress,
                        inputLimit,
                        outputBase,
                        outputAddress,
                        outputLimit,
                        CompressionParameters.DEFAULT_COMPRESSION_LEVEL);
                output.position(output.position() + written);
            }
        }
    }

    @Override
    public byte[] compress(byte[] input, int inputOffset, int inputLength)
    {
        byte[] output = new byte[maxCompressedLength(inputLength)];
        int length = compress(input, inputOffset, inputLength, output, 0, output.length);
        return Arrays.copyOfRange(output, 0, length);
    }

    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset)
    {
        return compress(input, inputOffset, inputLength, output, outputOffset, maxCompressedLength(inputLength));
    }
}