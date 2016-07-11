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

package org.apache.nifi.text;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;

@Tags({"text", "freeform", "expression", "language", "el", "resultset", "writer", "serialize"})
@CapabilityDescription("Writes the contents of a Database ResultSet as free-form text. The configured "
    + "text is able to make use of the Expression Language to reference each of the columns that are available "
    + "in the ResultSet. Each record in the ResultSet will be separated by a single newline character.")
public class FreeFormTextRecordSetWriter extends AbstractControllerService implements RecordSetWriterFactory {
    static final PropertyDescriptor TEXT = new PropertyDescriptor.Builder()
        .name("Text")
        .description("The text to use when writing the results. This property will evaluate the Expression Language using any of the columns available to the Result Set. For example, if the "
            + "following SQL Query is used: \"SELECT Name, COUNT(*) AS Count\" then the Expression can reference \"Name\" and \"Count\", such as \"${Name:toUpper()} ${Count:minus(1)}\"")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .required(true)
        .build();
    static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("The Character set to use when writing the data to the FlowFile")
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .defaultValue("UTF-8")
        .expressionLanguageSupported(false)
        .required(true)
        .build();

    private volatile PropertyValue textValue;
    private volatile Charset characterSet;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(TEXT);
        properties.add(CHARACTER_SET);
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        textValue = context.getProperty(TEXT);
        characterSet = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger) {
        return new FreeFormTextWriter(textValue, characterSet);
    }

}
