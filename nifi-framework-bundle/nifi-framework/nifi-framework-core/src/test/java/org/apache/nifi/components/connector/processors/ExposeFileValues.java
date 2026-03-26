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

package org.apache.nifi.components.connector.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.connector.components.ComponentState;
import org.apache.nifi.components.connector.components.ConnectorMethod;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

public class ExposeFileValues extends AbstractProcessor {

    static final PropertyDescriptor FILE = new PropertyDescriptor.Builder()
        .name("File")
        .description("The name of the file whose contents will be exposed via a ConnectorMethod.")
        .required(true)
        .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
        .build();

    private volatile String filename;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(FILE);
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(FILE)) {
            this.filename = newValue;
        }
    }

    @ConnectorMethod(
        name = "getFileValues",
        description = "Reads all lines from the configured file and returns them as a List of String.",
        allowedStates = ComponentState.STOPPED
    )
    public List<String> getFileValues() throws IOException {
        final File file = new File(filename);
        return Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
    }

    @Override
    public void onTrigger(final ProcessContext processContext, final ProcessSession processSession) throws ProcessException {

    }
}
