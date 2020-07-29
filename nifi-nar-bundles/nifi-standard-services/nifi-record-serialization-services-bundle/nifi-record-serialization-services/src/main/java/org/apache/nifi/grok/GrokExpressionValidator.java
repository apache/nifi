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

package org.apache.nifi.grok;

import io.krakens.grok.api.GrokCompiler;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

public class GrokExpressionValidator implements Validator {
    private GrokCompiler grokCompiler;
    private String patternFileName;

    public GrokExpressionValidator(String patternFileName, GrokCompiler compiler) {
        this.patternFileName = patternFileName;
        this.grokCompiler = compiler;
    }

    public GrokExpressionValidator() {
        this.grokCompiler = GrokCompiler.newInstance();
    }

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
        try {
            try (final InputStream in = getClass().getResourceAsStream(GrokReader.DEFAULT_PATTERN_NAME);
                 final Reader reader = new InputStreamReader(in)) {
                grokCompiler.register(in);
            }

            if (patternFileName != null) {
                try (final InputStream in = new FileInputStream(new File(patternFileName));
                     final Reader reader = new InputStreamReader(in)) {
                    grokCompiler.register(reader);
                }
            }
            grokCompiler.compile(input);
        } catch (final Exception e) {
            return new ValidationResult.Builder()
                .input(input)
                .subject(subject)
                .valid(false)
                .explanation("Invalid Grok pattern: " + e.getMessage())
                .build();
        }

        return new ValidationResult.Builder()
            .input(input)
            .subject(subject)
            .valid(true)
            .build();
    }

}
