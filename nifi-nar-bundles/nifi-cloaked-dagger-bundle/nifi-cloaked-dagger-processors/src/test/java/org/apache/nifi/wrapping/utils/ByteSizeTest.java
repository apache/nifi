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
package org.apache.nifi.wrapping.utils;

import static org.junit.Assert.assertArrayEquals;

import org.apache.nifi.wrapping.unwrapper.Unwrapper;
import org.apache.nifi.wrapping.wrapper.Wrapper;
import org.apache.nifi.wrapping.wrapper.WrapperFactory;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.TechniqueType;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class ByteSizeTest {
    /*
     * Technique 1
     */
    @Test
    public void test100bytes() throws Exception {
        byte[] expectedBytes = "abcedfghijklmnopqrstuvwxyabcedfghijklmnopqrstuvwxyabcedfghijklmnopqrstuvwxyabcedfghijklmnopqrstuvwxy"
                        .getBytes("UTF-8");
        byte[] actualBytes = wrapAndUnwrap(expectedBytes, TechniqueType.TECHNIQUE_1);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    public void test25bytes() throws Exception {
        byte[] expectedBytes = "abcedfghijklmnopqrstuvwxy".getBytes("UTF-8");
        byte[] actualBytes = wrapAndUnwrap(expectedBytes, TechniqueType.TECHNIQUE_1);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    public void test21bytes() throws Exception {
        byte[] expectedBytes = "abcedfghijklmnopqrstu".getBytes("UTF-8");
        byte[] actualBytes = wrapAndUnwrap(expectedBytes, TechniqueType.TECHNIQUE_1);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    public void test16bytes() throws Exception {
        byte[] expectedBytes = "abcedfghijklmnop".getBytes("UTF-8");
        byte[] actualBytes = wrapAndUnwrap(expectedBytes, TechniqueType.TECHNIQUE_1);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    public void test15bytes() throws Exception {
        byte[] expectedBytes = "abcedfghijklmno".getBytes("UTF-8");
        byte[] actualBytes = wrapAndUnwrap(expectedBytes, TechniqueType.TECHNIQUE_1);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    public void test17bytes() throws Exception {
        byte[] expectedBytes = "abcedfghijklmnopq".getBytes("UTF-8");
        byte[] actualBytes = wrapAndUnwrap(expectedBytes, TechniqueType.TECHNIQUE_1);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    /*
     * Technique 2
     */
    @Test
    public void t2test100bytes() throws Exception {
        byte[] expectedBytes = "abcedfghijklmnopqrstuvwxyabcedfghijklmnopqrstuvwxyabcedfghijklmnopqrstuvwxyabcedfghijklmnopqrstuvwxy"
                        .getBytes("UTF-8");
        byte[] actualBytes = wrapAndUnwrap(expectedBytes, TechniqueType.TECHNIQUE_2);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    public void t2test25bytes() throws Exception {
        byte[] expectedBytes = "abcedfghijklmnopqrstuvwxy".getBytes("UTF-8");
        byte[] actualBytes = wrapAndUnwrap(expectedBytes, TechniqueType.TECHNIQUE_2);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    public void t2test21bytes() throws Exception {
        byte[] expectedBytes = "abcedfghijklmnopqrstu".getBytes("UTF-8");
        byte[] actualBytes = wrapAndUnwrap(expectedBytes, TechniqueType.TECHNIQUE_2);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    public void t2test16bytes() throws Exception {
        byte[] expectedBytes = "abcedfghijklmnop".getBytes("UTF-8");
        byte[] actualBytes = wrapAndUnwrap(expectedBytes, TechniqueType.TECHNIQUE_2);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    public void t2test15bytes() throws Exception {
        byte[] expectedBytes = "abcedfghijklmno".getBytes("UTF-8");
        byte[] actualBytes = wrapAndUnwrap(expectedBytes, TechniqueType.TECHNIQUE_2);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    public void t2test17bytes() throws Exception {
        byte[] expectedBytes = "abcedfghijklmnopq".getBytes("UTF-8");
        byte[] actualBytes = wrapAndUnwrap(expectedBytes, TechniqueType.TECHNIQUE_2);
        assertArrayEquals(expectedBytes, actualBytes);
    }

    private byte[] wrapAndUnwrap(final byte[] bytes, final TechniqueType technique) throws Exception {
        final ByteArrayInputStream initialStream = new ByteArrayInputStream(bytes);
        final ByteArrayOutputStream wrappedOutputStream = new ByteArrayOutputStream();

        wrap(initialStream, wrappedOutputStream, technique);

        final ByteArrayInputStream wrappedInputStream = new ByteArrayInputStream(wrappedOutputStream.toByteArray());
        final ByteArrayOutputStream unwrappedOutputStream = new ByteArrayOutputStream();

        unwrap(wrappedInputStream, unwrappedOutputStream);

        return unwrappedOutputStream.toByteArray();
    }

    private void wrap(final InputStream inputStream, final OutputStream outputStream, final TechniqueType technique)
        throws Exception {
        final WrapperFactory wrapperFactory = new WrapperFactory();
        final Wrapper wrapper = wrapperFactory.getWrapper(outputStream, technique);

        final byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            wrapper.write(buffer, 0, bytesRead);
        }

        wrapper.close();

        outputStream.close();
    }

    private void unwrap(final InputStream inputStream, final OutputStream outputStream) throws Exception {
        final Unwrapper unwrapper = new Unwrapper(inputStream, outputStream);
        final byte[] buffer = new byte[8192];
        while (unwrapper.read(buffer) != -1) {
            // Do nothing here - the crazy unwrapper writes data straight to the
            // output stream and even decides to close if for us.
        }
        unwrapper.close();
        outputStream.close();
    }
}
