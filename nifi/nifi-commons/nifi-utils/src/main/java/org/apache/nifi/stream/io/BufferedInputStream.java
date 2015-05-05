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

import java.io.InputStream;

/**
 * This class is a slight modification of the BufferedInputStream in the java.io package. The modification is that this implementation does not provide synchronization on method calls, which means
 * that this class is not suitable for use by multiple threads. However, the absence of these synchronized blocks results in potentially much better performance.
 */
public class BufferedInputStream extends java.io.BufferedInputStream {

    public BufferedInputStream(final InputStream in) {
        super(in);
    }

    public BufferedInputStream(final InputStream in, final int size) {
        super(in, size);
    }
}
