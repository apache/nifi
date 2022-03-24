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

package org.apache.nifi.toolkit.tls.manager.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.nifi.toolkit.tls.util.OutputStreamFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Will write the object in JSON format
 *
 * @param <T> the type of object
 */
public class JsonConfigurationWriter<T> implements ConfigurationWriter<T> {
    private final ObjectWriter objectWriter;
    private final File file;

    public JsonConfigurationWriter(ObjectMapper objectMapper, File file) {
        this.objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
        this.file = file;
    }

    @Override
    public void write(T tlsConfig, OutputStreamFactory outputStreamFactory) throws IOException {
        try (OutputStream stream = outputStreamFactory.create(file)) {
            objectWriter.writeValue(stream, tlsConfig);
        }
    }
}
