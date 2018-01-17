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

package org.apache.nifi.wali;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

/**
 * A wrapper around a DataOutputStream, which wraps a ByteArrayOutputStream.
 * This allows us to obtain the DataOutputStream itself so that we can perform
 * writeXYZ methods and also allows us to obtain the underlying ByteArrayOutputStream
 * for performing methods such as size(), reset(), writeTo()
 */
public class ByteArrayDataOutputStream {
    private final ByteArrayOutputStream baos;
    private final DataOutputStream dos;

    public ByteArrayDataOutputStream(final int intiialBufferSize) {
        this.baos = new ByteArrayOutputStream(intiialBufferSize);
        this.dos = new DataOutputStream(baos);
    }

    public DataOutputStream getDataOutputStream() {
        return dos;
    }

    public ByteArrayOutputStream getByteArrayOutputStream() {
        return baos;
    }
}
