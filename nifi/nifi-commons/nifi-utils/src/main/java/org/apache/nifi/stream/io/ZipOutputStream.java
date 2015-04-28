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

import java.io.OutputStream;

/**
 * This class extends the {@link java.util.zip.ZipOutputStream} by providing a constructor that allows the user to specify the compression level. The default compression level is 1, as opposed to
 * Java's default of 5.
 */
public class ZipOutputStream extends java.util.zip.ZipOutputStream {

    public static final int DEFAULT_COMPRESSION_LEVEL = 1;

    public ZipOutputStream(final OutputStream out) {
        this(out, DEFAULT_COMPRESSION_LEVEL);
    }

    public ZipOutputStream(final OutputStream out, final int compressionLevel) {
        super(out);
        def.setLevel(compressionLevel);
    }
}
