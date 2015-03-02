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
package org.apache.nifi.provenance.journaling.journals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.nifi.remote.io.CompressionInputStream;
import org.apache.nifi.remote.io.CompressionOutputStream;

public class DeflatorCompressionCodec implements CompressionCodec {
    public static final String DEFLATOR_COMPRESSION_CODEC = "deflator-compression-codec";
    
    @Override
    public String getName() {
        return DEFLATOR_COMPRESSION_CODEC;
    }

    @Override
    public CompressedOutputStream newCompressionOutputStream(final OutputStream out) throws IOException {
        return new DeflatorOutputStream(out);
    }

    @Override
    public InputStream newCompressionInputStream(final InputStream in) throws IOException {
        return new CompressionInputStream(in);
    }

    
    private static class DeflatorOutputStream extends CompressedOutputStream {
        private final OutputStream originalOut;
        private CompressionOutputStream compressionOutput;
        
        public DeflatorOutputStream(final OutputStream out) {
            this.originalOut = out;
        }
        
        private void verifyState() {
            if ( compressionOutput == null ) {
                throw new IllegalStateException("No Compression Block has been created");
            }
        }
        
        @Override
        public void write(final int b) throws IOException {
            verifyState();
            compressionOutput.write(b);
        }
        
        @Override
        public void write(final byte[] b) throws IOException {
            verifyState();
            compressionOutput.write(b);
        }
        
        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            verifyState();
            compressionOutput.write(b, off, len);
        }
        
        @Override
        public void flush() throws IOException {
            if ( compressionOutput != null ) {
                compressionOutput.flush();
            }
        }
        
        @Override
        public void close() throws IOException {
            if ( compressionOutput != null ) {
                compressionOutput.close();
            }
            
            originalOut.close();
        }
        
        @Override
        public void beginNewBlock() throws IOException {
            compressionOutput = new CompressionOutputStream(originalOut);
        }
        
        @Override
        public void finishBlock() throws IOException {
            // Calling close() on CompressionOutputStream doesn't close the underlying stream -- it is designed
            // such that calling close() will write out the Compression footer and become unusable but not
            // close the underlying stream because the whole point of CompressionOutputStream as opposed to
            // GZIPOutputStream is that with CompressionOutputStream we can concatenate many together on a single
            // stream.
            if ( compressionOutput == null ) {
                return;
            } else {
                compressionOutput.close();
            }
        }
    }
    
}
