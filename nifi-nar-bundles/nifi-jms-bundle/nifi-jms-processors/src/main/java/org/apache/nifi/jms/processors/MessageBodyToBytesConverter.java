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
package org.apache.nifi.jms.processors;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.apache.commons.io.IOUtils;

/**
 *
 */
abstract class MessageBodyToBytesConverter {

    /**
     *
     * @param message instance of {@link TextMessage}
     * @return  byte array representing the {@link TextMessage}
     */
    public static byte[] toBytes(TextMessage message) {
        return MessageBodyToBytesConverter.toBytes(message, null);
    }

    /**
     *
     * @param message instance of {@link TextMessage}
     * @param charset character set used to interpret the TextMessage
     * @return  byte array representing the {@link TextMessage}
     */
    public static byte[] toBytes(TextMessage message, Charset charset) {
        try {
            if (charset == null) {
                return message.getText().getBytes();
            } else {
                return message.getText().getBytes(charset);
            }
        } catch (JMSException e) {
            throw new MessageConversionException("Failed to convert BytesMessage to byte[]", e);
        }
    }

    /**
     *
     * @param message instance of {@link BytesMessage}
     * @return byte array representing the {@link BytesMessage}
     */
    public static byte[] toBytes(BytesMessage message){
        try {
            InputStream is = new BytesMessageInputStream(message);
            return IOUtils.toByteArray(is);
        } catch (Exception e) {
            throw new MessageConversionException("Failed to convert BytesMessage to byte[]", e);
        }
    }


    private static class BytesMessageInputStream extends InputStream {
        private BytesMessage message;

        public BytesMessageInputStream(BytesMessage message) {
            this.message = message;
        }

        @Override
        public int read() throws IOException {
            try {
                return this.message.readByte();
            } catch (JMSException e) {
                throw new IOException(e.toString());
            }
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            try {
                if (offset == 0) {
                    return this.message.readBytes(buffer, length);
                } else {
                    return super.read(buffer, offset, length);
                }
            } catch (JMSException e) {
                throw new IOException(e.toString());
            }
        }

        @Override
        public int read(byte[] buffer) throws IOException {
            try {
                return this.message.readBytes(buffer);
            } catch (JMSException e) {
                throw new IOException(e.toString());
            }
        }
    }


    static class MessageConversionException extends RuntimeException {
        private static final long serialVersionUID = -1464448549601643887L;

        public MessageConversionException(String msg) {
            super(msg);
        }

        public MessageConversionException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
}
