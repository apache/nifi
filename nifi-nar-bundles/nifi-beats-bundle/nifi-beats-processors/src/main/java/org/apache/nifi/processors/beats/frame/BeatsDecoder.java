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
package org.apache.nifi.processors.beats.frame;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.InflaterInputStream;
import org.apache.nifi.logging.ComponentLog;

/**
 * Decodes a Beats frame by maintaining a state based on each byte that has been processed. This class
 * should not be shared by multiple threads.
 */
public class BeatsDecoder {


    final ComponentLog logger;

    private BeatsFrame.Builder frameBuilder;
    private BeatsState currState = BeatsState.VERSION;
    private byte decodedFrameType;

    private byte[] unprocessedData;

    private final Charset charset;
    private final ByteArrayOutputStream currBytes;

    private long windowSize;

    static final int MIN_FRAME_HEADER_LENGTH = 2; // Version + Type
    static final int WINDOWSIZE_LENGTH = MIN_FRAME_HEADER_LENGTH + 4; // 32bit unsigned window size
    static final int COMPRESSED_MIN_LENGTH = MIN_FRAME_HEADER_LENGTH + 4; // 32 bit unsigned + payload
    static final int JSON_MIN_LENGTH = MIN_FRAME_HEADER_LENGTH + 8; // 32 bit unsigned sequence number + 32 bit unsigned payload length

    public static final byte FRAME_WINDOWSIZE = 0x57, FRAME_DATA = 0x44, FRAME_COMPRESSED = 0x43, FRAME_ACK = 0x41, FRAME_JSON = 0x4a;

    /**
     * @param charset the charset to decode bytes from the frame
     */
    public BeatsDecoder(final Charset charset, final ComponentLog logger) {
        this(charset, new ByteArrayOutputStream(4096), logger);
    }

    /**
     * @param charset the charset to decode bytes from the frame
     * @param buffer  a buffer to use while processing the bytes
     */
    public BeatsDecoder(final Charset charset, final ByteArrayOutputStream buffer, final ComponentLog logger) {
        this.logger = logger;
        this.charset = charset;
        this.currBytes = buffer;
        this.frameBuilder = new BeatsFrame.Builder();
        this.decodedFrameType = 0x00;
    }

    /**
     * Resets this decoder back to its initial state.
     */
    public void reset() {
        frameBuilder = new BeatsFrame.Builder();
        currState = BeatsState.VERSION;
        decodedFrameType = 0x00;
        currBytes.reset();
    }

    /**
     * Process the next byte from the channel, updating the builder and state accordingly.
     *
     * @param currByte the next byte to process
     * @preturn true if a frame is ready to be retrieved, false otherwise
     */
    public boolean process(final byte currByte) throws BeatsFrameException {
        try {
            switch (currState) {
                case VERSION: // Just enough data to process the version
                    processVERSION(currByte);
                    break;
                case FRAMETYPE: // Also able to process the frametype
                    processFRAMETYPE(currByte);
                    break;
                case PAYLOAD: // Initial bytes with version and Frame Type have already been received, start iteration over payload
                    processPAYLOAD(currByte);

                    // At one stage, the data sent to processPAYLOAD will be represente a complete frame, so we check before returning true

                    if (frameBuilder.frameType == FRAME_WINDOWSIZE && currState == BeatsState.COMPLETE) {
                        return true;
                    } else if (frameBuilder.frameType == FRAME_COMPRESSED && currState == BeatsState.COMPLETE) {
                        return true;
                    } else if (frameBuilder.frameType == FRAME_JSON && currState == BeatsState.COMPLETE) {
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
            throw new BeatsFrameException("Error decoding Beats frame: " + e.getMessage(), e);
        }
    }


    /**
     * Returns the decoded frame and resets the decoder for the next frame.
     * This method should be called after checking isComplete().
     *
     * @return the BeatsFrame that was decoded
     */
    public List<BeatsFrame> getFrames() throws BeatsFrameException {
        List<BeatsFrame> frames = new LinkedList<>();

        if (currState != BeatsState.COMPLETE) {
            throw new BeatsFrameException("Must be at the trailer of a frame");
        }
        try {
            // Once compressed frames are expanded, they must be devided into individual frames
            if (currState == BeatsState.COMPLETE && frameBuilder.frameType == FRAME_COMPRESSED) {
                logger.debug("Frame is compressed, will iterate to decode", new Object[]{});

                // Zero currBytes, currState and frameBuilder prior to iteration over
                // decompressed bytes
                currBytes.reset();
                frameBuilder.reset();
                currState = BeatsState.VERSION;

                // Run over decompressed data and split frames
                frames = splitCompressedFrames(unprocessedData);

            // In case of V or wired D and J frames we just ship them across the List
            } else {
                final BeatsFrame frame = frameBuilder.build();
                currBytes.reset();
                frameBuilder.reset();
                currState = BeatsState.VERSION;
                frames.add(frame);
            }
            return frames;

        } catch (Exception e) {
            throw new BeatsFrameException("Error decoding Beats frame: " + e.getMessage(), e);
        }
    }

    private List<BeatsFrame> splitCompressedFrames(byte[] decompressedData) {
        List<BeatsFrame> frames = new LinkedList<>();
        BeatsFrame.Builder internalFrameBuilder = new BeatsFrame.Builder();
        ByteBuffer currentData = ByteBuffer.wrap(decompressedData);

        // Both Lumberjack v1 and Beats (LJ v2) has a weird approach to frames, where compressed frames embed D(ata) or J(SON) frames.
        // inside a compressed input.
        //  Or as stated in the documentation:
        //
        // "As an example, you could have 3 data frames compressed into a single
        // 'compressed' frame type: 1D{k,v}{k,v}1D{k,v}{k,v}1D{k,v}{k,v}"
        //
        // Therefore, instead of calling process method again, just iterate over each of
        // the frames and split them so they can be processed by BeatsFrameHandler

        while (currentData.hasRemaining()) {

            int payloadLength = 0;

            internalFrameBuilder.version = currentData.get();
            internalFrameBuilder.frameType = currentData.get();
            switch (internalFrameBuilder.frameType) {
                case FRAME_JSON:

                    internalFrameBuilder.seqNumber = (int) (currentData.getInt() & 0x00000000ffffffffL);
                    currentData.mark();

                    internalFrameBuilder.dataSize = currentData.getInt() & 0x00000000ffffffffL;
                    currentData.mark();

                    // Define how much data to chomp
                    payloadLength = Math.toIntExact(internalFrameBuilder.dataSize);
                    byte[] jsonBytes = new byte[payloadLength];

                    currentData.get(jsonBytes, 0, payloadLength);
                    currentData.mark();

                    // Add payload to frame
                    internalFrameBuilder.payload(jsonBytes);
                    break;
            }

            // data frame is created
            BeatsFrame frame = internalFrameBuilder.build();
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
        currState = BeatsState.FRAMETYPE;
    }

    private void processFRAMETYPE(final byte b) {
        decodedFrameType = b;
        frameBuilder.frameType(decodedFrameType);
        logger.debug("Frame type is {}", new Object[]{decodedFrameType});
        currBytes.write(b);
        currState = BeatsState.PAYLOAD;
    }


    /** Process the outer PAYLOAD byte by byte. Once data is read state is set to COMPLETE so that the data payload
     * can be processed fully using {@link #splitCompressedFrames(byte[])}
     * */
    private void processPAYLOAD(final byte b) {
        currBytes.write(b);
        switch (decodedFrameType) {
            case FRAME_WINDOWSIZE: //'W'
                if (currBytes.size() < WINDOWSIZE_LENGTH ) {
                    logger.trace("Beats currBytes contents are {}", new Object[] {currBytes.toString()});
                    break;
                } else if (currBytes.size() == WINDOWSIZE_LENGTH) {
                    frameBuilder.dataSize = ByteBuffer.wrap(java.util.Arrays.copyOfRange(currBytes.toByteArray(), 2, 6)).getInt() & 0x00000000ffffffffL;
                    logger.debug("Data size is {}", new Object[]{frameBuilder.dataSize});
                    // Sets payload  to empty as frame contains no data
                    frameBuilder.payload(new byte[]{});
                    currBytes.reset();
                    currState = BeatsState.COMPLETE;
                    windowSize = frameBuilder.dataSize;
                    break;
                } else { // Should never be here to be honest...
                    logger.debug("Saw a packet I should not have seen. Packet contents were {}", new Object[] {currBytes.toString()});
                    break;
                }
            case FRAME_COMPRESSED: //'C'
                if (currBytes.size() < COMPRESSED_MIN_LENGTH) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Beats currBytes contents are {}", new Object[] {currBytes.toString()});
                    }
                    break;
                } else if (currBytes.size() >= COMPRESSED_MIN_LENGTH) {
                    // If data contains more thant the minimum data size
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
                            unprocessedData = out.toByteArray();
                            // buf is no longer needed
                            buf = null;
                            logger.debug("Finished decompressing data");
                            // Decompression is complete, we should be able to proceed with resetting currBytes and curSrtate and iterating them
                            // as type 'D' frames
                            frameBuilder.dataSize(unprocessedData.length);
                            currState = BeatsState.COMPLETE;

                        } catch (IOException e) {
                            throw new BeatsFrameException("Error decompressing  frame: " + e.getMessage(), e);
                        }

                    }
                    break;
                    // If currentByte.size is not lower than six and also not equal or great than 6...
                } else { // Should never be here to be honest...
                    if (logger.isDebugEnabled()) {
                        logger.debug("Received a compressed frame with partial data or invalid content. The packet contents were {}", new Object[] {currBytes.toString()});
                    }
                    break;
                }
            case FRAME_JSON: // 'J́'
                // Because Beats can disable compression, sometimes, JSON data will be received outside a compressed
                // stream (i.e. 0x43). Instead of processing it here, we defer its processing to went getFrames is
                // called
                if (currBytes.size() < JSON_MIN_LENGTH) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Beats currBytes contents are {}", new Object[] {currBytes.toString()});
                    }
                    break;
                } else if (currBytes.size() == JSON_MIN_LENGTH) {
                    // Read the sequence number from bytes
                    frameBuilder.seqNumber = (int) (ByteBuffer.wrap(java.util.Arrays.copyOfRange(currBytes.toByteArray(), 2, 6)).getInt() & 0x00000000ffffffffL);
                    // Read the JSON payload length
                    frameBuilder.dataSize = ByteBuffer.wrap(java.util.Arrays.copyOfRange(currBytes.toByteArray(), 6, 10)).getInt() & 0x00000000ffffffffL;
                } else if (currBytes.size() > JSON_MIN_LENGTH) {
                    // Wait for payload to be fully read and then complete processing
                    if (currBytes.size() - 10 == frameBuilder.dataSize) {
                        // Transfer the current payload so it can be processed by {@link #splitCompressedFrames} method.
                        frameBuilder.payload = java.util.Arrays.copyOfRange(currBytes.toByteArray(), 10, currBytes.size());
                        currState = BeatsState.COMPLETE;
                    }
                    break;
                }
        }
    }

}
