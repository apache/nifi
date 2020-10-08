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

package org.apache.nifi.reporting;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.apache.nifi.components.Validator.VALID;
import static org.apache.nifi.expression.ExpressionLanguageScope.VARIABLE_REGISTRY;
import static org.apache.nifi.processor.util.StandardValidators.FILE_EXISTS_VALIDATOR;

public class WriteToFileReportingTask extends AbstractReportingTask {

    static final PropertyDescriptor FILENAME = new Builder()
        .name("Filename")
        .displayName("Filename")
        .description("The File to write to")
        .required(true)
        .addValidator(FILE_EXISTS_VALIDATOR)
        .expressionLanguageSupported(VARIABLE_REGISTRY)
        .build();
    static final PropertyDescriptor TEXT = new Builder()
        .name("Text")
        .displayName("Text")
        .description("The Text to Write")
        .required(true)
        .addValidator(VALID)
        .expressionLanguageSupported(VARIABLE_REGISTRY)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(FILENAME, TEXT);
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final File outFile = new File(context.getProperty(FILENAME).evaluateAttributeExpressions().getValue());
        final String text = context.getProperty(TEXT).evaluateAttributeExpressions().getValue();

        try (final FileOutputStream fos = new FileOutputStream(outFile)) {
            fos.write(text.getBytes(StandardCharsets.UTF_8));
        } catch (final IOException e) {
            throw new ProcessException(e);
        }

        getLogger().info("Wrote text to file {}", new Object[] {outFile.getAbsolutePath()});
    }
}
