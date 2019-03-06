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
package org.apache.nifi.wrapping.unwrapper;

import java.io.IOException;

/**
 * Interface for Unwrapping Techniques.
 */
public interface TechUnwrapper {
    /**
     * Read and unwrap a single byte to an output stream.
     *
     * @return the number of bytes read.
     * @throws IOException
     *             - for problems during the read.
     */
    int read() throws IOException;

    /**
     * Reads and Unwraps <code>len</code> bytes from the specified <code>byte</code> array starting at offset
     * <code>off</code> to an output stream.
     *
     * @param src
     *            - byte array to be read.
     * @param off
     *            - offset for starting point in array.
     * @param len
     *            - the maximum number of bytes to read.
     * @return number of bytes read.
     * @throws IOException
     *             - for problems during the read.
     */
    int read(byte[] src, int off, int len) throws IOException;

    /**
     * Reads and Unwraps the specified byte array to an output stream.
     *
     * @param src
     *            - byte array to be read.
     * @return number of bytes read.
     * @throws IOException
     *             - for problems during the read.
     */
    int read(byte[] src) throws IOException;
}
