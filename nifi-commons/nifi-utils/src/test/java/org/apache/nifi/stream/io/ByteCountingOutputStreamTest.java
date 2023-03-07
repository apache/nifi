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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;


class ByteCountingOutputStreamTest {

    @Test
    void testReset() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(100);
        ByteCountingOutputStream bcos = new ByteCountingOutputStream(baos);
        bcos.write("Hello".getBytes(StandardCharsets.UTF_8));
        assertEquals(5, bcos.getBytesWritten());
        bcos.reset();
        assertEquals(0, bcos.getBytesWritten());
    }
}