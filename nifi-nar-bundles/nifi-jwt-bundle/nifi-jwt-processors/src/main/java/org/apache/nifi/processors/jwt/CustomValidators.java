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
package org.apache.nifi.processors.jwt;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;

import java.io.File;
import java.io.IOException;
import java.security.PrivateKey;

public class CustomValidators {
    static PrivateKey privateKey = null;

    public static final Validator DIRECTORY_HAS_PUBLIC_KEYS_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            // expression language
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input)
                        .explanation("Expression Language Present").valid(true).build();
            }
            // not empty
            ValidationResult nonEmptyValidatorResult = StandardValidators.NON_EMPTY_VALIDATOR.validate(subject, input,
                    context);
            if (!nonEmptyValidatorResult.isValid()) {
                return nonEmptyValidatorResult;
            }
            // valid path
            ValidationResult pathValidatorResult = StandardValidators.FILE_EXISTS_VALIDATOR.validate(subject, input,
                    context);
            if (!pathValidatorResult.isValid()) {
                return pathValidatorResult;
            }
            // path is directory
            final File directory = new File(input);
            if (!directory.isDirectory()) {
                return new ValidationResult.Builder().subject(subject).input(input)
                        .explanation(subject + " must be a directory").valid(false).build();
            }
            // directory contains at least one ".pub" file
            final File[] keys = directory.listFiles((dir, name) -> name.toLowerCase().endsWith(".pub"));
            if (keys.length < 1) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation(
                        "the directory must contain at least one RSA public key file with the entension \".pub\"")
                        .valid(false).build();
            }
            return new ValidationResult.Builder().subject(subject).input(input)
                    .explanation("Valid public key directory").valid(true).build();
        }
    };

    public static final Validator PRIVATE_KEY_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            // expression language
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input)
                        .explanation("Expression Language Present").valid(true).build();
            }
            // not empty
            ValidationResult nonEmptyValidatorResult = StandardValidators.NON_EMPTY_VALIDATOR.validate(subject, input,
                    context);
            if (!nonEmptyValidatorResult.isValid()) {
                return nonEmptyValidatorResult;
            }
            // valid path
            ValidationResult pathValidatorResult = StandardValidators.FILE_EXISTS_VALIDATOR.validate(subject, input,
                    context);
            if (!pathValidatorResult.isValid()) {
                return pathValidatorResult;
            }
            // is a file
            final File key = new File(input);
            if (!key.isFile()) {
                return new ValidationResult.Builder().subject(subject).input(input)
                        .explanation(subject + " must be path to private key, not a directory").valid(false).build();
            }
            // file is readable
            if (!key.canRead()) {
                return new ValidationResult.Builder().subject(subject).input(input)
                        .explanation(subject + " must be readable by the NiFi user").valid(false).build();
            }
            // read key from file
            final SSHClient sshClient = new SSHClient();
            KeyProvider keyProvider;
            try {
                keyProvider = sshClient.loadKeys(key.getAbsolutePath());
                privateKey = keyProvider.getPrivate();
                sshClient.close();
            } catch (IOException e) {
                return new ValidationResult.Builder().subject(subject).input(input)
                        .explanation(subject + " is not a valid private key file").valid(false).build();
            }
            return new ValidationResult.Builder().subject(subject).input(input)
                    .explanation("Valid file").valid(true).build();
        }
    };
}