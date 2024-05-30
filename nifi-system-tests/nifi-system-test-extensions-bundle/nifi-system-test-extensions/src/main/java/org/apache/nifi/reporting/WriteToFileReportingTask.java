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
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.cs.tests.system.CountService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.nifi.components.Validator.VALID;
import static org.apache.nifi.expression.ExpressionLanguageScope.VARIABLE_REGISTRY;

public class WriteToFileReportingTask extends AbstractReportingTask {

    static final PropertyDescriptor FILENAME = new Builder()
        .name("Filename")
        .displayName("Filename")
        .description("The File to write to")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(VARIABLE_REGISTRY)
        .build();
    static final PropertyDescriptor TEXT = new Builder()
        .name("Text")
        .displayName("Text")
        .description("The Text to Write")
        .required(false)
        .addValidator(VALID)
        .expressionLanguageSupported(VARIABLE_REGISTRY)
        .build();
    static final PropertyDescriptor COUNT_SERVICE = new Builder()
        .name("Count Service")
        .displayName("Count Service")
        .description("The Count Service to Use")
        .required(false)
        .identifiesControllerService(CountService.class)
        .build();

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        if (validationContext.getProperty(COUNT_SERVICE).isSet() && validationContext.getProperty(TEXT).isSet()) {
            return Collections.singleton(new ValidationResult.Builder()
                .subject("Count Service and Text")
                .valid(false)
                .explanation("Cannot set both the Text property and the Count Service property")
                .build());
        }
        if (!validationContext.getProperty(COUNT_SERVICE).isSet() && !validationContext.getProperty(TEXT).isSet()) {
            return Collections.singleton(new ValidationResult.Builder()
                .subject("Count Service and Text")
                .valid(false)
                .explanation("Either the Text property or the Count Service property must be set")
                .build());
        }

        return Collections.emptyList();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(FILENAME, TEXT, COUNT_SERVICE);
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final File outFile = new File(context.getProperty(FILENAME).evaluateAttributeExpressions().getValue());
        final File parentFile = outFile.getParentFile();
        if (!parentFile.exists() && !parentFile.mkdirs()) {
            getLogger().error("Could not create directory {}", parentFile.getAbsolutePath());
            return;
        }

        String text = context.getProperty(TEXT).evaluateAttributeExpressions().getValue();
        if (text == null) {
            final CountService countService = context.getProperty(COUNT_SERVICE).asControllerService(CountService.class);
            final long count = countService.count();
            text = String.valueOf(count);
        }

        try (final FileOutputStream fos = new FileOutputStream(outFile)) {
            fos.write(text.getBytes(StandardCharsets.UTF_8));
        } catch (final IOException e) {
            throw new ProcessException(e);
        }

        getLogger().info("Wrote text to file {}", outFile.getAbsolutePath());
    }
}
