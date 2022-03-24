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
package org.apache.nifi.stream.io;

import java.io.IOException;
import java.io.OutputStream;

/**
 * <p>
 * This class extends the {@link java.util.zip.GZIPOutputStream} by allowing the constructor to provide a compression level, and uses a default value of 1, rather than 5.
 * </p>
 */
public class GZIPOutputStream extends java.util.zip.GZIPOutputStream {

    public static final int DEFAULT_COMPRESSION_LEVEL = 1;

    public GZIPOutputStream(final OutputStream out) throws IOException {
        this(out, DEFAULT_COMPRESSION_LEVEL);
    }

    public GZIPOutputStream(final OutputStream out, final int compressionLevel) throws IOException {
        super(out);
        def.setLevel(compressionLevel);
    }
}
