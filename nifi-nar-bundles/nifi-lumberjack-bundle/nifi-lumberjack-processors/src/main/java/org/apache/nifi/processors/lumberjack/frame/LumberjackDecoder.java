/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.lumberjack.frame;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.InflaterInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes a Lumberjack frame by maintaining a state based on each byte that has been processed. This class
 * should not be shared by multiple threads.
 */
@Deprecated
public class LumberjackDecoder {

    static final Logger logger = LoggerFactory.getLogger(LumberjackDecoder.class);

    private LumberjackFrame.Builder frameBuilder;
    private LumberjackState currState = LumberjackState.VERSION;
    private byte decodedFrameType;

    private byte[] decompressedData;

    private final Charset charset;
    private final ByteArrayOutputStream currBytes;

    private long windowSize;

    public static final byte FRAME_WINDOWSIZE = 0x57, FRAME_DATA = 0x44, FRAME_COMPRESSED = 0x43, FRAME_ACK = 0x41, FRAME_JSON = 0x4a;

    /**
     * @param charset the charset to decode bytes from the Lumberjack frame
     */
    public LumberjackDecoder(final Charset charset) {
        this(charset, new ByteArrayOutputStream(4096));
    }

    /**
     * @param charset the charset to decode bytes from the Lumberjack frame
     * @param buffer  a buffer to use while processing the bytes
     */
    public LumberjackDecoder(final Charset charset, final ByteArrayOutputStream buffer) {
        this.charset = charset;
        this.currBytes = buffer;
        this.frameBuilder = new LumberjackFrame.Builder();
        this.decodedFrameType = 0x00;
    }

    /**
     * Resets this decoder back to its initial state.
     */
    public void reset() {
        frameBuilder = new LumberjackFrame.Builder();
        currState = LumberjackState.VERSION;
        decodedFrameType = 0x00;
        currBytes.reset();
    }

    /**
     * Process the next byte from the channel, updating the builder and state accordingly.
     *
     * @param currByte the next byte to process
     * @preturn true if a frame is ready to be retrieved, false otherwise
     */
    public boolean process(final byte currByte) throws LumberjackFrameException {
        try {
            switch (currState) {
                case VERSION:
                    processVERSION(currByte);
                    break;
                case FRAMETYPE:
                    processFRAMETYPE(currByte);
                    break;
                case PAYLOAD:
                    processPAYLOAD(currByte);
                    if (frameBuilder.frameType == FRAME_WINDOWSIZE && currState == LumberjackState.COMPLETE) {
                        return true;
                    } else if (frameBuilder.frameType == FRAME_COMPRESSED && currState == LumberjackState.COMPLETE) {
                        return true;
                    } else {
                        break;
                    }
                case COMPLETE:
                    return true;
                default:
                    break;
            }
            return false;
        } catch (Exception e) {
            throw new LumberjackFrameException("Error decoding Lumberjack frame: " + e.getMessage(), e);
        }
    }


    /**
     * Returns the decoded frame and resets the decoder for the next frame.
     * This method should be called after checking isComplete().
     *
     * @return the LumberjackFrame that was decoded
     */
    public List<LumberjackFrame> getFrames() throws LumberjackFrameException {
        List<LumberjackFrame> frames = new LinkedList<>();

        if (currState != LumberjackState.COMPLETE) {
            throw new LumberjackFrameException("Must be at the trailer of a frame");
        }
        try {
            if (currState == LumberjackState.COMPLETE && frameBuilder.frameType == FRAME_COMPRESSED) {
                logger.debug("Frame is compressed, will iterate to decode", new Object[]{});
                // LumberjackDecoder decompressedDecoder = new LumberjackDecoder();

                // Zero currBytes, currState and frameBuilder prior to iteration over
                // decompressed bytes
                currBytes.reset();
                frameBuilder.reset();
                currState = LumberjackState.VERSION;

                // Run over decompressed data.
                frames = processDECOMPRESSED(decompressedData);

            } else {
                final LumberjackFrame frame = frameBuilder.build();
                currBytes.reset();
                frameBuilder.reset();
                currState = LumberjackState.VERSION;
                frames.add(frame);
            }
            return frames;

        } catch (Exception e) {
            throw new LumberjackFrameException("Error decoding Lumberjack frame: " + e.getMessage(), e);
        }
    }

    private List<LumberjackFrame> processDECOMPRESSED(byte[] decompressedData) {
        List<LumberjackFrame> frames = new LinkedList<>();
        LumberjackFrame.Builder internalFrameBuilder = new LumberjackFrame.Builder();
        ByteBuffer currentData = ByteBuffer.wrap(decompressedData);

        // Lumberjack has a weird approach to frames, where compressed frames embed D(ata) or J(SON) frames.
        // inside a compressed input.
        //  Or as stated in the documentation:
        //
        // "As an example, you could have 3 data frames compressed into a single
        // 'compressed' frame type: 1D{k,v}{k,v}1D{k,v}{k,v}1D{k,v}{k,v}"
        //
        // Therefore, instead of calling process method again, just iterate over each of
        // the frames and split them so they can be processed by LumberjackFrameHandler

        while (currentData.hasRemaining()) {

            int payloadLength = 0;

            internalFrameBuilder.version = currentData.get();
            internalFrameBuilder.frameType = currentData.get();
            internalFrameBuilder.seqNumber = currentData.getInt() & 0x00000000ffffffffL;
            currentData.mark();

            // Set the payloadLength to negative to avoid doing math
            // around valueLength and valueLength
            payloadLength = payloadLength - currentData.position();

            long pairCount = currentData.getInt() & 0x00000000ffffffffL;
            for (int i = 0; i < pairCount; i++) {
                long keyLength = currentData.getInt() & 0x00000000ffffffffL;
                currentData.position(currentData.position() + (int) keyLength);
                long valueLength = currentData.getInt() & 0x00000000ffffffffL;
                currentData.position(currentData.position() + (int) valueLength);
            }
            // Infer the length of the payload from position...
            payloadLength = payloadLength + currentData.position();

            // Reset to mark (i.e. skip frame headers) prior to getting the data
            currentData.reset();

            // get the data, shift mark and compact so next iteration can
            // read rest of buffer.
            byte[] bytes = new byte[payloadLength];
            currentData.get(bytes, 0, payloadLength);
            currentData.mark();

            // Add payload to frame
            internalFrameBuilder.payload(bytes);

            // data frame is created
            LumberjackFrame frame = internalFrameBuilder.build();
            frames.add(frame);
            internalFrameBuilder.reset();
        }

        return frames;
    }


    private void processVERSION(final byte b) {
        byte version = b;
        frameBuilder.version(version);
        logger.debug("Version number is {}", new Object[]{version});
        currBytes.write(b);
        currState = LumberjackState.FRAMETYPE;
    }

    private void processFRAMETYPE(final byte b) {
        decodedFrameType = b;
        frameBuilder.frameType(decodedFrameType);
        logger.debug("Frame type is {}", new Object[]{decodedFrameType});
        currBytes.write(b);
        currState = LumberjackState.PAYLOAD;
    }

    private void processPAYLOAD(final byte b) {
        currBytes.write(b);
        switch (decodedFrameType) {
            case FRAME_WINDOWSIZE: //'W'
                if (currBytes.size() < 6) {
                    logger.trace("Lumberjack currBytes contents are {}", currBytes.toString());
                    break;
                } else if (currBytes.size() == 6) {
                    frameBuilder.dataSize = ByteBuffer.wrap(java.util.Arrays.copyOfRange(currBytes.toByteArray(), 2, 6)).getInt() & 0x00000000ffffffffL;
                    logger.debug("Data size is {}", new Object[]{frameBuilder.dataSize});
                    // Sets payload  to empty as frame contains no data
                    frameBuilder.payload(new byte[]{});
                    currBytes.reset();
                    currState = LumberjackState.COMPLETE;
                    windowSize = frameBuilder.dataSize;
                    break;
                } else {
                    break;
                }
            case FRAME_COMPRESSED: //'C'
                if (currBytes.size() < 6) {
                    logger.trace("Lumberjack currBytes contents are {}", currBytes.toString());
                    break;
                } else if (currBytes.size() >= 6) {
                    frameBuilder.dataSize = ByteBuffer.wrap(java.util.Arrays.copyOfRange(currBytes.toByteArray(), 2, 6)).getInt() & 0x00000000ffffffffL;
                    if (currBytes.size() - 6 == frameBuilder.dataSize) {
                        try {
                            byte[] buf = java.util.Arrays.copyOfRange(currBytes.toByteArray(), 6, currBytes.size());
                            InputStream in = new InflaterInputStream(new ByteArrayInputStream(buf));
                            ByteArrayOutputStream out = new ByteArrayOutputStream();
                            byte[] buffer = new byte[1024];
                            int len;
                            while ((len = in.read(buffer)) > 0) {
                                out.write(buffer, 0, len);
                            }
                            in.close();
                            out.close();
                            decompressedData = out.toByteArray();
                            // buf is no longer needed
                            buf = null;
                            logger.debug("Finished decompressing data");
                            // Decompression is complete, we should be able to proceed with resetting currBytes and curSrtate and iterating them
                            // as type 'D' frames
                            frameBuilder.dataSize(decompressedData.length);
                            currState = LumberjackState.COMPLETE;

                        } catch (IOException e) {
                            throw new LumberjackFrameException("Error decompressing  frame: " + e.getMessage(), e);
                        }

                    }
                    break;

                    // If currentByte.size is not lower than six and also not equal or great than 6...
                } else {
                    break;
                }
        }
    }

    private void processCOMPLETE() {
        currBytes.reset();
        frameBuilder.reset();
        currState = LumberjackState.VERSION;
    }

}
