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
package org.apache.nifi.processors.standard.relp.frame;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes a RELP frame by maintaining a state based on each byte that has been processed. This class
 * should not be shared by multiple threads.
 */
public class RELPDecoder {

    static final Logger logger = LoggerFactory.getLogger(RELPDecoder.class);

    private RELPFrame.Builder frameBuilder;
    private RELPState currState = RELPState.TXNR;

    private final Charset charset;
    private final ByteArrayOutputStream currBytes;

    /**
     * @param charset the charset to decode bytes from the RELP frame
     */
    public RELPDecoder(final Charset charset) {
        this(charset, new ByteArrayOutputStream(4096));
    }

    /**
     *
     * @param charset the charset to decode bytes from the RELP frame
     * @param buffer a buffer to use while processing the bytes
     */
    public RELPDecoder(final Charset charset, final ByteArrayOutputStream buffer) {
        this.charset = charset;
        this.currBytes = buffer;
        this.frameBuilder = new RELPFrame.Builder();
    }

    /**
     * Resets this decoder back to it's initial state.
     */
    public void reset() {
        frameBuilder = new RELPFrame.Builder();
        currState = RELPState.TXNR;
        currBytes.reset();
    }

    /**
     * Process the next byte from the channel, updating the builder and state accordingly.
     *
     * @param currByte the next byte to process
     * @preturn true if a frame is ready to be retrieved, false otherwise
     */
    public boolean process(final byte currByte) throws RELPFrameException {
        try {
            switch (currState) {
                case TXNR:
                    processTXNR(currByte);
                    break;
                case COMMAND:
                    processCOMMAND(currByte);
                    break;
                case LENGTH:
                    processLENGTH(currByte);
                    // if jumped from length to trailer we need to return true here
                    // because there might not be another byte to process
                    if (currState == RELPState.TRAILER) {
                        return true;
                    }
                    break;
                case DATA:
                    processDATA(currByte);
                    break;
                case TRAILER:
                    return true;
                default:
                    break;
            }
            return false;
        } catch (Exception e) {
            throw new RELPFrameException("Error decoding RELP frame: " + e.getMessage(), e);
        }
    }

    /**
     * Returns the decoded frame and resets the decoder for the next frame.
     * This method should be called after checking isComplete().
     *
     * @return the RELPFrame that was decoded
     */
    public RELPFrame getFrame() throws RELPFrameException {
        if (currState != RELPState.TRAILER) {
            throw new RELPFrameException("Must be at the trailer of a frame");
        }

        try {
            final RELPFrame frame = frameBuilder.build();
            processTRAILER(RELPFrame.DELIMITER);
            return frame;
        } catch (Exception e) {
            throw new RELPFrameException("Error decoding RELP frame: " + e.getMessage(), e);
        }
    }


    private void processTXNR(final byte b) {
        if (b == RELPFrame.SEPARATOR) {
            if (currBytes.size() > 0) {
                final long txnr = Long.parseLong(new String(currBytes.toByteArray(), charset));
                frameBuilder.txnr(txnr);
                logger.debug("Transaction number is {}", new Object[]{txnr});

                currBytes.reset();
                currState = RELPState.COMMAND;
            }
        } else {
            currBytes.write(b);
        }
    }

    private void processCOMMAND(final byte b) {
        if (b == RELPFrame.SEPARATOR) {
            final String command = new String(currBytes.toByteArray(), charset);
            frameBuilder.command(command);
            logger.debug("Command is {}", new Object[] {command});

            currBytes.reset();
            currState = RELPState.LENGTH;
        } else {
            currBytes.write(b);
        }
    }

    private void processLENGTH(final byte b) {
        if (b == RELPFrame.SEPARATOR || (currBytes.size() > 0 && b == RELPFrame.DELIMITER)) {
            final int dataLength = Integer.parseInt(new String(currBytes.toByteArray(), charset));
            frameBuilder.dataLength(dataLength);
            logger.debug("Length is {}", new Object[] {dataLength});

            currBytes.reset();

            // if at a separator then data is going to follow, but if at a separator there is no data
            if (b == RELPFrame.SEPARATOR) {
                currState = RELPState.DATA;
            } else {
                frameBuilder.data(new byte[0]);
                currState = RELPState.TRAILER;
            }
        } else {
            currBytes.write(b);
        }
    }

    private void processDATA(final byte b) {
        currBytes.write(b);
        logger.trace("Data size is {}", new Object[] {currBytes.size()});

        if (currBytes.size() >= frameBuilder.dataLength) {
            final byte[] data = currBytes.toByteArray();
            frameBuilder.data(data);
            logger.debug("Reached expected data size of {}", new Object[] {frameBuilder.dataLength});

            currBytes.reset();
            currState = RELPState.TRAILER;
        }
    }

    private void processTRAILER(final byte b) {
        if (b != RELPFrame.DELIMITER) {
            logger.warn("Expected RELP trailing LF, but found another byte");
        }
        currBytes.reset();
        frameBuilder.reset();
        currState = RELPState.TXNR;
    }

}
