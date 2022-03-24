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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageEOFException;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationUtils;

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
            if (message.getText() == null) {
                return new byte[0];
            }
            if (charset == null) {
                return message.getText().getBytes();
            } else {
                return message.getText().getBytes(charset);
            }
        } catch (JMSException e) {
            throw new MessageConversionException("Failed to convert " + TextMessage.class.getSimpleName() + " to byte[]", e);
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
            throw new MessageConversionException("Failed to convert " + BytesMessage.class.getSimpleName() + " to byte[]", e);
        }
    }

    /**
     *
     * @param message instance of {@link ObjectMessage}
     * @return byte array representing the {@link ObjectMessage}
     */
    public static byte[] toBytes(ObjectMessage message) {
        try {
            return SerializationUtils.serialize(message.getObject());
        } catch (Exception e) {
            throw new MessageConversionException("Failed to convert " + ObjectMessage.class.getSimpleName() + " to byte[]", e);
        }
    }

    /**
     * @param message instance of {@link StreamMessage}
     * @return byte array representing the {@link StreamMessage}
     */
    public static byte[] toBytes(StreamMessage message) {
        try (
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        ) {
            while (true) {
                try {
                    Object element = message.readObject();
                    if (element instanceof Boolean) {
                        dataOutputStream.writeBoolean((Boolean) element);
                    } else if (element instanceof byte[]) {
                        dataOutputStream.write((byte[]) element);
                    } else if (element instanceof Byte) {
                        dataOutputStream.writeByte((Byte) element);
                    } else if (element instanceof Short) {
                        dataOutputStream.writeShort((Short) element);
                    } else if (element instanceof Integer) {
                        dataOutputStream.writeInt((Integer) element);
                    } else if (element instanceof Long) {
                        dataOutputStream.writeLong((Long) element);
                    } else if (element instanceof Float) {
                        dataOutputStream.writeFloat((Float) element);
                    } else if (element instanceof Double) {
                        dataOutputStream.writeDouble((Double) element);
                    } else if (element instanceof Character) {
                        dataOutputStream.writeChar((Character) element);
                    } else if (element instanceof String) {
                        dataOutputStream.writeUTF((String) element);
                    } else {
                        throw new MessageConversionException("Unsupported type in " + StreamMessage.class.getSimpleName() + ": '" + element.getClass() + "'");
                    }
                } catch (MessageEOFException mEofE) {
                    break;
                }
            }

            dataOutputStream.flush();

            byte[] bytes = byteArrayOutputStream.toByteArray();

            return bytes;
        } catch (Exception e) {
            throw new MessageConversionException("Failed to convert " + StreamMessage.class.getSimpleName() + " to byte[]", e);
        }
    }

    /**
     * @param message instance of {@link MapMessage}
     * @return byte array representing the {@link MapMessage}
     */
    public static byte[] toBytes(MapMessage message) {
        ObjectMapper objectMapper = new ObjectMapper();

        try (
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ) {
            Map<String, Object> objectMap = new HashMap<>();

            Enumeration mapNames = message.getMapNames();

            while (mapNames.hasMoreElements()) {
                String name = (String) mapNames.nextElement();
                Object value = message.getObject(name);
                if (value instanceof byte[]) {
                    byte[] bytes = (byte[]) value;
                    List<Byte> byteList = new ArrayList<>(bytes.length);
                    for (byte aByte : bytes) {
                        byteList.add(aByte);
                    }
                    objectMap.put(name, byteList);
                } else {
                    objectMap.put(name, value);
                }
            }

            objectMapper.writeValue(byteArrayOutputStream, objectMap);

            byte[] jsonAsByteArray = byteArrayOutputStream.toByteArray();

            return jsonAsByteArray;
        } catch (JMSException e) {
            throw new MessageConversionException("Couldn't read incoming " + MapMessage.class.getSimpleName(), e);
        } catch (IOException e) {
            throw new MessageConversionException("Couldn't transform incoming " + MapMessage.class.getSimpleName() + " to JSON", e);
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
