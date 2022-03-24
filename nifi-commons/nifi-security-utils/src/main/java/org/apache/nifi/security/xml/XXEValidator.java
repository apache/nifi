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
package org.apache.nifi.security.xml;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* This validator is a quick method to block XML files being used which contain external entity ("!ENTITY") declarations.
 */
public class XXEValidator implements Validator {
    private static final Logger logger = LoggerFactory.getLogger(XXEValidator.class);

    // This pattern looks complicated but allows for arbitrary space/newlines between characters in the <!ENTITY element
    private final Pattern xxePattern = Pattern.compile("(?i)<\\s*!\\s*E\\s*N\\s*T\\s*I\\s*T\\s*Y");

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
        Path xmlFilePath = Paths.get(input);
        String line;
        boolean containsXXE = false;

        if (validationContext.isExpressionLanguageSupported(subject) && validationContext.isExpressionLanguagePresent(input)) {
            return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
        }

        final String xmlFilePathString = xmlFilePath.toString();
        logger.info("Validating {} for XXE attack", xmlFilePathString);

        if (Files.exists(xmlFilePath)) {
            try (BufferedReader reader = Files.newBufferedReader(xmlFilePath)) {
                StringBuilder sb = new StringBuilder();

                while ((line = reader.readLine()) != null) {
                    Matcher matcher = xxePattern.matcher(line);

                    if (matcher.find()) {
                        // We found an external entity declaration. Stop reading the file and return an invalid property result.
                        containsXXE = true;
                        logger.warn("Detected XXE attack in {}", xmlFilePathString);
                        break;
                    }

                    sb.append(line).append("\n");
                }

                // Some attacks will be crafted with newlines between the characters which trigger the attack
                if (!containsXXE) {
                    logger.debug("No XXE attack detected in {} line-by-line; checking concatenated document", xmlFilePathString);
                    Matcher matcher = xxePattern.matcher(sb.toString());
                    containsXXE = matcher.find();
                    if (containsXXE) {
                        logger.warn("Detected multiline XXE attack in {}", xmlFilePathString);
                    } else {
                        logger.debug("No XXE attack detected in full file {}", xmlFilePathString);
                    }
                }

                if (containsXXE) {
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                            .explanation("XML file " + input + " contained an external entity. To prevent XXE vulnerabilities, NiFi has external entity processing disabled.")
                            .build();
                } else {
                    return new ValidationResult.Builder().subject(subject).input(input).valid(true).explanation("No XXE attack detected.").build();
                }
            } catch (IOException e) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                        .explanation(input + " is not valid because: " + e.getLocalizedMessage())
                        .build();
            }
        } else {
            return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                    .explanation("File not found: " + input + " could not be found.")
                    .build();
        }
    }
}