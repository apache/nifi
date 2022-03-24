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
package org.apache.nifi.toolkit.cli.impl.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;

import java.io.IOException;
import java.io.OutputStream;

public class JacksonUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        MAPPER.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL));
        MAPPER.setAnnotationIntrospector(new JaxbAnnotationIntrospector(MAPPER.getTypeFactory()));
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private static final ObjectWriter OBJECT_WRITER = MAPPER.writerWithDefaultPrettyPrinter();

    public static ObjectMapper getObjectMapper() {
        return MAPPER;
    }

    public static ObjectWriter getObjectWriter() {
        return OBJECT_WRITER;
    }

    public static void write(final Object result, final OutputStream output) throws IOException {
        OBJECT_WRITER.writeValue(new OutputStream() {
            @Override
            public void write(byte[] b) throws IOException {
                output.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                output.write(b, off, len);
            }

            @Override
            public void write(int b) throws IOException {
                output.write(b);
            }

            @Override
            public void close() throws IOException {
                // DON'T close the output stream here
                output.flush();
            }
        }, result);
    }
}
