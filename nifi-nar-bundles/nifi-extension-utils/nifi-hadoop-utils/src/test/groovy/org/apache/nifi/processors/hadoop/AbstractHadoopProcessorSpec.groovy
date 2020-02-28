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
package org.apache.nifi.processors.hadoop

import org.apache.nifi.components.PropertyValue
import org.apache.nifi.components.ValidationContext
import org.apache.nifi.hadoop.KerberosProperties
import org.apache.nifi.kerberos.KerberosCredentialsService
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.ProcessorInitializationContext
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.util.MockComponentLog
import org.apache.nifi.util.MockPropertyValue
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.Unroll

class AbstractHadoopProcessorSpec extends Specification {
    private static Logger LOGGER = LoggerFactory.getLogger AbstractHadoopProcessorSpec

    @Unroll
    def "customValidate for #testName"() {
        given:
        def testAbstractHadoopProcessor = new TestAbstractHadoopProcessor(allowExplicitKeytab);
        testAbstractHadoopProcessor.kerberosProperties = new KerberosProperties(new File('src/test/resources/krb5.conf'))
        def mockProcessorInitializationContext = Mock ProcessorInitializationContext
        def mockValidationContext = Mock ValidationContext
        def mockHadoopConfigurationResourcesPropertyValue = Mock PropertyValue
        def mockKerberosCredentialsServicePropertyValue = Mock PropertyValue
        def mockKerberosCredentialsServiceControllerService = Mock KerberosCredentialsService

        when:
        testAbstractHadoopProcessor.initialize(mockProcessorInitializationContext)
        def validationResults = testAbstractHadoopProcessor.customValidate(mockValidationContext)

        then:
        1 * mockProcessorInitializationContext.getLogger() >> new MockComponentLog("AbstractHadoopProcessorSpec", testAbstractHadoopProcessor)
        1 * mockValidationContext.getProperty(AbstractHadoopProcessor.HADOOP_CONFIGURATION_RESOURCES) >> mockHadoopConfigurationResourcesPropertyValue
        1 * mockHadoopConfigurationResourcesPropertyValue.evaluateAttributeExpressions() >> mockHadoopConfigurationResourcesPropertyValue
        1 * mockHadoopConfigurationResourcesPropertyValue.getValue() >> "src/test/resources/test-secure-core-site.xml"

        1 * mockValidationContext.getProperty(AbstractHadoopProcessor.KERBEROS_CREDENTIALS_SERVICE) >> mockKerberosCredentialsServicePropertyValue
        if (configuredKeytabCredentialsService) {
            1 * mockKerberosCredentialsServicePropertyValue.asControllerService(KerberosCredentialsService.class) >> mockKerberosCredentialsServiceControllerService
            1 * mockKerberosCredentialsServiceControllerService.principal >> configuredKeytabCredentialsServicePrincipal
            1 * mockKerberosCredentialsServiceControllerService.keytab >> configuredKeytabCredentialsServiceKeytab
        }

        1 * mockValidationContext.getProperty(testAbstractHadoopProcessor.kerberosProperties.kerberosPrincipal) >> new MockPropertyValue(configuredPrincipal)
        1 * mockValidationContext.getProperty(testAbstractHadoopProcessor.kerberosProperties.kerberosPassword) >> new MockPropertyValue(configuredPassword)
        1 * mockValidationContext.getProperty(testAbstractHadoopProcessor.kerberosProperties.kerberosKeytab) >> new MockPropertyValue(configuredKeytab)

        then:
        def actualValidationErrors = validationResults.each { !it.isValid() }
        if (actualValidationErrors.size() > 0) {
            actualValidationErrors.each { LOGGER.debug(it.toString()) }
        }
        actualValidationErrors.size() == expectedValidationErrorCount

        where:
        testName | configuredPrincipal | configuredKeytab | configuredPassword | allowExplicitKeytab | configuredKeytabCredentialsService | configuredKeytabCredentialsServicePrincipal | configuredKeytabCredentialsServiceKeytab || expectedValidationErrorCount
        "success case 1"       | "principal"         | "keytab"         | null               | "true"              | false                              | null                                        | null                                     || 0
        "success case 2"       | "principal"         | null             | "password"         | "true"              | false                              | null                                        | null                                     || 0
        "success case 3"       | "principal"         | null             | "password"         | "false"             | false                              | null                                        | null                                     || 0
        "success case 4"       | null                | null             | null               | "true"              | true                               | "principal"                                 | "keytab"                                 || 0
        "success case 5"       | null                | null             | null               | "false"             | true                               | "principal"                                 | "keytab"                                 || 0
        // do not allow explicit keytab, but provide one anyway; validation fails
        "failure case 1"       | "principal"         | "keytab"         | null               | "false"             | false                              | null                                        | null                                     || 1
        "failure case 2"       | null                | "keytab"         | null               | "false"             | false                              | null                                        | null                                     || 2
        // keytab credentials service is provided, but explicit properties for principal, password, or keytab are also provided; validation fails
        "failure case 3"       | "principal"         | null             | null               | "true"              | true                               | "principal"                                 | "keytab"                                 || 1
        "failure case 4"       | null                | "keytab"         | null               | "true"              | true                               | "principal"                                 | "keytab"                                 || 1
        "failure case 5"       | null                | null             | "password"         | "true"              | true                               | "principal"                                 | "keytab"                                 || 2
        "failure case 6"       | "principal"         | null             | null               | "false"             | true                               | "principal"                                 | "keytab"                                 || 1
        "failure case 7"       | null                | "keytab"         | null               | "false"             | true                               | "principal"                                 | "keytab"                                 || 2
        "failure case 8"       | null                | null             | "password"         | "false"             | true                               | "principal"                                 | "keytab"                                 || 2
    }

    private class TestAbstractHadoopProcessor extends AbstractHadoopProcessor {
        def allowExplicitKeytab = false

        TestAbstractHadoopProcessor(def allowExplicitKeytab) {
            this.allowExplicitKeytab = allowExplicitKeytab
        }

        @Override
        void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
            throw new NoSuchMethodError("not intended to be invoked by the test, this implementation is only intended for custom validation purposes")
        }

        @Override
        String getAllowExplicitKeytabEnvironmentVariable() {
            allowExplicitKeytab
        }

    }
}
