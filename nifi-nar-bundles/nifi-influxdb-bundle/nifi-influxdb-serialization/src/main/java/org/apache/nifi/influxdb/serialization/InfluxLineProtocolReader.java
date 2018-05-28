/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.influxdb.serialization;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Tags({"influxdb", "measurement", "insert", "parse", "record", "reader", "record", "timeseries"})
@CapabilityDescription("This Processor parses the InfluxDB Line Protocol data to a record. "
        + "This is useful for listening data from the Telegraf, InfluxDB or IoT. "
        + "For details and examples of configuration see additional information.")
public class InfluxLineProtocolReader extends AbstractControllerService implements RecordReaderFactory {

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("line-protocol-character-set")
            .displayName("Character Set")
            .description("The Character Encoding that is used to decode the Line Protocol data")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue(StandardCharsets.UTF_8.name())
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;

    static {

        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();

        propertyDescriptors.add(CHARSET);

        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(propertyDescriptors);
    }

    private volatile String charsetName;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {

        this.charsetName = context.getProperty(CHARSET).getValue();
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables,
                                           final InputStream in,
                                           final ComponentLog logger)

            throws MalformedRecordException, IOException, SchemaNotFoundException {

        Charset charset = Charset.forName(charsetName);

        return new InfluxLineProtocolRecordReader(in, charset);
    }
}
