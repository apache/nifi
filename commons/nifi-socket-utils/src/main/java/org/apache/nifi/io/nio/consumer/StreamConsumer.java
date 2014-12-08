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
package org.apache.nifi.io.nio.consumer;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.nifi.io.nio.BufferPool;

/**
 * A StreamConsumer must be thread safe. It may be accessed concurrently by a
 * thread providing data to process and another thread that is processing that
 * data.
 *
 * @author none
 */
public interface StreamConsumer {

    /**
     * Will be called once just after construction. It provides the queue to
     * which processed and emptied and cleared buffers must be returned. For
     * each time <code>addFilledBuffer</code> is called there should be an
     * associated add to this given queue. If not, buffers will run out and all
     * stream processing will halt. READ THIS!!!
     *
     * @param returnQueue
     */
    void setReturnBufferQueue(BufferPool returnQueue);

    /**
     * Will be called by the thread that produces byte buffers with available
     * data to be processed. If the consumer is finished this should simply
     * return the given buffer to the return buffer queue (after it is cleared)
     *
     * @param buffer
     */
    void addFilledBuffer(ByteBuffer buffer);

    /**
     * Will be called by the thread that executes the consumption of data. May
     * be called many times though once <code>isConsumerFinished</code> returns
     * true this method will likely do nothing.
     * @throws java.io.IOException
     */
    void process() throws IOException;

    /**
     * Called once the end of the input stream is detected
     */
    void signalEndOfStream();

    /**
     * If true signals the consumer is done consuming data and will not process
     * any more buffers.
     *
     * @return
     */
    boolean isConsumerFinished();

    /**
     * Uniquely identifies the consumer
     *
     * @return
     */
    String getId();

}
