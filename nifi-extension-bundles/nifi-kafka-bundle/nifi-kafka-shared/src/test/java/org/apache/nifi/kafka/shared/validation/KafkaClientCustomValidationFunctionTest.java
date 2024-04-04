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
package org.apache.nifi.kafka.shared.validation;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.kafka.shared.component.KafkaClientComponent;
import org.apache.nifi.kafka.shared.property.KafkaClientProperty;
import org.apache.nifi.kafka.shared.property.SaslMechanism;
import org.apache.nifi.kafka.shared.property.SecurityProtocol;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaClientCustomValidationFunctionTest {

    private static final String JAAS_CONFIG_JNDI_LOGIN_MODULE = "com.sun.security.auth.module.JndiLoginModule required debug=true;";
    private static final String JAAS_CONFIG_PLACEHOLDER = "jaas.config";

    TestRunner runner;

    KafkaClientCustomValidationFunction validationFunction;

    @BeforeEach
    void setValidationFunction() {
        System.clearProperty(KafkaClientCustomValidationFunction.JAVA_SECURITY_AUTH_LOGIN_CONFIG);
        validationFunction = new KafkaClientCustomValidationFunction();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
    }

    @Test
    void testApply() {
        final ValidationContext validationContext = getValidationContext();
        final Collection<ValidationResult> results = validationFunction.apply(validationContext);

        assertTrue(results.isEmpty());
    }

    @Test
    void testApplyKerberosSaslWithoutCredentialsInvalid() {
        runner.setProperty(KafkaClientComponent.SECURITY_PROTOCOL, SecurityProtocol.SASL_PLAINTEXT.name());
        runner.setProperty(KafkaClientComponent.SASL_MECHANISM, SaslMechanism.GSSAPI);

        final ValidationContext validationContext = getValidationContext();
        final Collection<ValidationResult> results = validationFunction.apply(validationContext);

        assertPropertyValidationResultFound(results, KafkaClientComponent.SASL_MECHANISM.getDisplayName());
    }

    @Test
    void testApplyKerberosSaslSystemPropertyWithoutServiceNameInvalid() {
        runner.setProperty(KafkaClientComponent.SECURITY_PROTOCOL, SecurityProtocol.SASL_PLAINTEXT.name());
        runner.setProperty(KafkaClientComponent.SASL_MECHANISM, SaslMechanism.GSSAPI);

        System.setProperty(KafkaClientCustomValidationFunction.JAVA_SECURITY_AUTH_LOGIN_CONFIG, JAAS_CONFIG_PLACEHOLDER);

        final ValidationContext validationContext = getValidationContext();
        final Collection<ValidationResult> results = validationFunction.apply(validationContext);

        assertPropertyValidationResultFound(results, KafkaClientComponent.KERBEROS_SERVICE_NAME.getDisplayName());
    }

    @Test
    void testApplyKerberosSaslSystemPropertyValid() {
        runner.setProperty(KafkaClientComponent.SECURITY_PROTOCOL, SecurityProtocol.SASL_PLAINTEXT.name());
        runner.setProperty(KafkaClientComponent.SASL_MECHANISM, SaslMechanism.GSSAPI);
        runner.setProperty(KafkaClientComponent.KERBEROS_SERVICE_NAME, KafkaClientComponent.KERBEROS_SERVICE_NAME.getName());

        System.setProperty(KafkaClientCustomValidationFunction.JAVA_SECURITY_AUTH_LOGIN_CONFIG, JAAS_CONFIG_PLACEHOLDER);

        final ValidationContext validationContext = getValidationContext();
        final Collection<ValidationResult> results = validationFunction.apply(validationContext);

        assertTrue(results.isEmpty());
    }

    @Test
    void testApplyPlainUsernameWithoutPasswordInvalid() {
        runner.setProperty(KafkaClientComponent.SASL_USERNAME, KafkaClientComponent.SASL_USERNAME.getName());
        runner.setProperty(KafkaClientComponent.SASL_MECHANISM, SaslMechanism.PLAIN);

        final ValidationContext validationContext = getValidationContext();
        final Collection<ValidationResult> results = validationFunction.apply(validationContext);

        assertPropertyValidationResultFound(results, KafkaClientComponent.SASL_PASSWORD.getDisplayName());
    }

    @Test
    void testApplyPlainPasswordWithoutUsernameInvalid() {
        runner.setProperty(KafkaClientComponent.SASL_PASSWORD, KafkaClientComponent.SASL_PASSWORD.getName());
        runner.setProperty(KafkaClientComponent.SASL_MECHANISM, SaslMechanism.PLAIN);

        final ValidationContext validationContext = getValidationContext();
        final Collection<ValidationResult> results = validationFunction.apply(validationContext);

        assertPropertyValidationResultFound(results, KafkaClientComponent.SASL_USERNAME.getDisplayName());
    }

    @Test
    void testApplySaslJaasConfigJndiLoginModuleInvalid() {
        runner.setProperty(KafkaClientProperty.SASL_JAAS_CONFIG.getProperty(), JAAS_CONFIG_JNDI_LOGIN_MODULE);

        final ValidationContext validationContext = getValidationContext();
        final Collection<ValidationResult> results = validationFunction.apply(validationContext);

        assertPropertyValidationResultFound(results, KafkaClientProperty.SASL_JAAS_CONFIG.getProperty());
    }

    private ValidationContext getValidationContext() {
        final MockProcessContext processContext = (MockProcessContext) runner.getProcessContext();
        return new MockValidationContext(processContext);
    }

    private void assertPropertyValidationResultFound(final Collection<ValidationResult> results, final String subject) {
        final Optional<ValidationResult> validationResult = results.stream()
                .filter(
                        result -> result.getSubject().equals(subject)
                ).findFirst();

        assertTrue(validationResult.isPresent());
    }
}
