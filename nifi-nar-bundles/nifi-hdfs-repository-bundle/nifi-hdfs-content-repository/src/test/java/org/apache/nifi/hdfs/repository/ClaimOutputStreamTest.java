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
package org.apache.nifi.hdfs.repository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.junit.Test;

public class ClaimOutputStreamTest {

    @Test
    public void normalTest() throws IOException {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

        ClaimClosedHandler handler = mock(ClaimClosedHandler.class);
        StandardContentClaim claim = new StandardContentClaim(mock(ResourceClaim.class), 0);

        ClaimOutputStream claimOut = new ClaimOutputStream(handler, claim, new ByteCountingOutputStream(bytesOut));

        byte[] line = "this is some test data!".getBytes(StandardCharsets.UTF_8);
        claimOut.write(line);
        claimOut.write('\n');
        claimOut.write(line, 0, 4);
        claimOut.write('\n');

        verify(handler, times(0)).claimClosed(any());

        claimOut.close();

        assertEquals(line.length + 2 + 4, claim.getLength());
        assertTrue(claimOut.canRecycle());
        verify(handler, times(1)).claimClosed(eq(claimOut));
    }

    @Test
    public void exceptionTest() throws IOException {
        OutputStream failingStream = new OutputStream() {
            @Override
            public void write(byte[] b) throws IOException {
                throw new IOException("expected test error");
            }
            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                throw new IOException("expected test error");
            }
            @Override
            public void write(int b) throws IOException {
                throw new IOException("expected test error");
            }
        };

        ClaimClosedHandler handler = mock(ClaimClosedHandler.class);
        StandardContentClaim claim = new StandardContentClaim(mock(ResourceClaim.class), 0);

        ClaimOutputStream claimOut1 = new ClaimOutputStream(handler, claim, new ByteCountingOutputStream(failingStream));
        try {
            claimOut1.write("this is a test".getBytes(StandardCharsets.UTF_8));
            fail("Expected failing stream to throw an exception");
        } catch (IOException ex) { }
        claimOut1.close();

        assertEquals(-1, claim.getLength());
        assertFalse(claimOut1.canRecycle());

        ClaimOutputStream claimOut2 = new ClaimOutputStream(handler, claim, new ByteCountingOutputStream(failingStream));
        try {
            claimOut2.write("this is a test".getBytes(StandardCharsets.UTF_8), 0, 4);
            fail("Expected failing stream to throw an exception");
        } catch (IOException ex) { }
        claimOut2.close();

        assertFalse(claimOut2.canRecycle());

        ClaimOutputStream claimOut3 = new ClaimOutputStream(handler, claim, new ByteCountingOutputStream(failingStream));
        try {
            claimOut3.write('\n');
            fail("Expected failing stream to throw an exception");
        } catch (IOException ex) { }
        claimOut3.close();

        assertFalse(claimOut3.canRecycle());

    }

}
