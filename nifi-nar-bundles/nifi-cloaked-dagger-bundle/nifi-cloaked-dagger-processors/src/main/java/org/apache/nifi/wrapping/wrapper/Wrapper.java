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
package org.apache.nifi.wrapping.wrapper;

import java.io.IOException;

/**
 * Interface for a Wrapper object.
 */
public interface Wrapper {
    /**
     * Wraps and writes the specified byte to this output stream.
     *
     * @param item
     *            - byte to output (given as an int)
     * @throws IOException
     *             - for problems during the write.
     */
    void write(int item) throws IOException;

    /**
     * Wraps and writes the specified byte array to this output stream.
     *
     * @param bytes
     *            - byte array to be written.
     * @throws IOException
     *             - for problems during the write.
     */
    void write(byte[] bytes) throws IOException;

    /**
     * Wraps and writes <code>len</code> bytes from the specified <code>byte</code> array starting at offset
     * <code>off</code> to this output stream.
     *
     * @param src
     *            - byte array to be written.
     * @param off
     *            - offset for starting point in array.
     * @param len
     *            - the maximum number of bytes to write.
     * @throws IOException
     *             - for problems during the write.
     */
    void write(byte[] src, int off, int len) throws IOException;

    /**
     * Close the output stream.
     *
     * @throws IOException
     *             - for problems during the close.
     */
    void close() throws IOException;

    /**
     * Flush the output stream.
     *
     * @throws IOException
     *             - for problems during the flush.
     */
    void flush() throws IOException;
}