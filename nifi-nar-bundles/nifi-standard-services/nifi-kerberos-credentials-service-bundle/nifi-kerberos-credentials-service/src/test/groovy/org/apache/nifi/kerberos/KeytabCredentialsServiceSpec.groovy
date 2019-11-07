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
package org.apache.nifi.kerberos

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.PropertyValue
import org.apache.nifi.controller.ConfigurationContext
import org.apache.nifi.controller.ControllerServiceInitializationContext
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.ProcessorInitializationContext
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.util.TestRunners
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicReference

class KeytabCredentialsServiceSpec extends Specification {

    @Unroll
    def "Get principal on '#configuredPrincipal' with strategy #strategy.displayName"() {
        given:
        def keytabCredentialsService = new KeytabCredentialsService()
        def mockControllerServiceInitializationContext = Mock(ControllerServiceInitializationContext)
        def mockConfigurationContext = Mock(ConfigurationContext)
        def mockKeytabPropertyValue = Mock(PropertyValue)
        def mockPrincipalPropertyValue = Mock(PropertyValue)
        def mockHostnamePropertyValue = Mock(PropertyValue)
        def mockFqdnPropertyValue = Mock(PropertyValue)
        def mockPrincipalInstanceQualifierStrategyPropertyValue = Mock(PropertyValue)
        def tmpKrbConf = File.createTempFile("krb5-tmp", ".conf")

        when:
        keytabCredentialsService.init(mockControllerServiceInitializationContext)
        keytabCredentialsService.setConfiguredValues(mockConfigurationContext)
        keytabCredentialsService.abstractStoreConfigContext(mockConfigurationContext)

        then:
        1 * mockControllerServiceInitializationContext.getKerberosConfigurationFile() >> tmpKrbConf

        1 * mockConfigurationContext.getProperty(KeytabCredentialsService.KEYTAB) >> mockKeytabPropertyValue
        1 * mockKeytabPropertyValue.evaluateAttributeExpressions() >> mockKeytabPropertyValue
        1 * mockKeytabPropertyValue.value >> "keytab.fake"

        1 * mockConfigurationContext.getProperty(KeytabCredentialsService.PRINCIPAL) >> mockPrincipalPropertyValue
        1 * mockPrincipalPropertyValue.evaluateAttributeExpressions() >> mockPrincipalPropertyValue
        1 * mockPrincipalPropertyValue.value >> configuredPrincipal

        1 * mockConfigurationContext.getProperty(KeytabCredentialsService.PRINCIPAL_INSTANCE_QUALIFIER_STRATEGY) >> mockPrincipalInstanceQualifierStrategyPropertyValue
        1 * mockPrincipalInstanceQualifierStrategyPropertyValue.value >> strategy.value

        when:
        hostnameQueries * mockConfigurationContext.getProperty(KeytabCredentialsService.NODE_HOSTNAME_EXPRESSION) >> mockHostnamePropertyValue
        hostnameQueries * mockHostnamePropertyValue.evaluateAttributeExpressions() >> mockHostnamePropertyValue
        hostnameQueries * mockHostnamePropertyValue.value >> "node1"

        fqdnQueries * mockConfigurationContext.getProperty(KeytabCredentialsService.NODE_FQDN_EXPRESSION) >> mockFqdnPropertyValue
        fqdnQueries * mockFqdnPropertyValue.evaluateAttributeExpressions() >> mockFqdnPropertyValue
        fqdnQueries * mockFqdnPropertyValue.value >> "node1.fakedomain.com"

        def actualPrincipal = keytabCredentialsService.getPrincipal()

        then:
        actualPrincipal == expectedPrincipal

        where:
        strategy                                                       | configuredPrincipal                     | hostnameQueries | fqdnQueries || expectedPrincipal
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_NONE     | "nifi"                                  | 0               | 0           || "nifi"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_NONE     | "nifi@EXAMPLE.COM"                      | 0               | 0           || "nifi@EXAMPLE.COM"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_NONE     | "nifi/node1"                            | 0               | 0           || "nifi/node1"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_NONE     | "nifi/node1@EXAMPLE.COM"                | 0               | 0           || "nifi/node1@EXAMPLE.COM"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_NONE     | "nifi/node1@EXAMPLE.COM"                | 0               | 0           || "nifi/node1@EXAMPLE.COM"

        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_HOSTNAME | "nifi"                                  | 1               | 0           || "nifi/node1"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_HOSTNAME | "nifi@EXAMPLE.COM"                      | 1               | 0           || "nifi/node1@EXAMPLE.COM"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_HOSTNAME | "nifi/node1"                            | 1               | 0           || "nifi/node1"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_HOSTNAME | "nifi/node1@EXAMPLE.COM"                | 1               | 0           || "nifi/node1@EXAMPLE.COM"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_HOSTNAME | "nifi/node1.fakedomain.com@EXAMPLE.COM" | 1               | 0           || "nifi/node1.fakedomain.com@EXAMPLE.COM"

        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_FQDN     | "nifi"                                  | 0               | 1           || "nifi/node1.fakedomain.com"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_FQDN     | "nifi@EXAMPLE.COM"                      | 0               | 1           || "nifi/node1.fakedomain.com@EXAMPLE.COM"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_FQDN     | "nifi/node1.fakedomain.com"             | 0               | 1           || "nifi/node1.fakedomain.com"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_FQDN     | "nifi/node1.fakedomain.com@EXAMPLE.COM" | 0               | 1           || "nifi/node1.fakedomain.com@EXAMPLE.COM"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_FQDN     | "nifi/node1@EXAMPLE.COM"                | 0               | 1           || "nifi/node1@EXAMPLE.COM"
    }

    @Unroll
    def "Test processor with KeytabCredentialsService using strategy #strategy.displayName"() {
        given:
        def keytabCredentialsServiceId = UUID.randomUUID().toString()
        def processor = new KerberosCredentialsServiceProcessor()
        def testRunner = TestRunners.newTestRunner(processor)
        def emptyKeytab = Files.createTempFile(Paths.get("./target"), "fake-", ".keytab")
        def emptyKrb5Conf = Files.createTempFile(Paths.get("./target"), "fake-", "-krb5.conf")
        def keytabCredentialsService = new KeytabCredentialsService()
        def customNiFiProperties = NiFiProperties.createBasicNiFiProperties(null, [(NiFiProperties.KERBEROS_KRB5_FILE): emptyKrb5Conf.toAbsolutePath().toString()])

        when:
        testRunner.addControllerService(
                keytabCredentialsServiceId,
                keytabCredentialsService,
                [(KeytabCredentialsService.PRINCIPAL.name)                            : configuredPrincipal,
                 (KeytabCredentialsService.KEYTAB.name)                               : emptyKeytab.toAbsolutePath().toString(),
                 (KeytabCredentialsService.PRINCIPAL_INSTANCE_QUALIFIER_STRATEGY.name): strategy.value],
                customNiFiProperties)
        testRunner.setProperty(KerberosCredentialsServiceProcessor.KERBEROS_CREDENTIALS_SERVICE, keytabCredentialsServiceId)
        testRunner.enableControllerService(keytabCredentialsService)

        then:
        noExceptionThrown()

        when:
        testRunner.assertValid()

        then:
        noExceptionThrown()

        when:
        testRunner.enqueue([] as byte[])
        testRunner.run()

        then:
        noExceptionThrown()
        processor.actualPrincipal.get() == expectedPrincipal

        where:
        strategy                                                       | configuredPrincipal                               || expectedPrincipal
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_NONE     | "nifi"                                            || "nifi"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_NONE     | "nifi@EXAMPLE.COM"                                || "nifi@EXAMPLE.COM"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_NONE     | "nifi/node1"                                      || "nifi/node1"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_NONE     | "nifi/node1@EXAMPLE.COM"                          || "nifi/node1@EXAMPLE.COM"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_NONE     | "nifi/node1.fakedomain.com@EXAMPLE.COM"           || "nifi/node1.fakedomain.com@EXAMPLE.COM"

        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_HOSTNAME | "nifi"                                            || "nifi/${InetAddress.localHost.hostName}" as String
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_HOSTNAME | "nifi@EXAMPLE.COM"                                || "nifi/${InetAddress.localHost.hostName}@EXAMPLE.COM" as String
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_HOSTNAME | "nifi/node1" as String                            || "nifi/node1" as String
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_HOSTNAME | "nifi/node1@EXAMPLE.COM" as String                || "nifi/node1@EXAMPLE.COM" as String
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_HOSTNAME | "nifi/node1.fakedomain.com@EXAMPLE.COM" as String || "nifi/node1.fakedomain.com@EXAMPLE.COM" as String

        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_FQDN     | "nifi"                                            || "nifi/${InetAddress.localHost.canonicalHostName}" as String
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_FQDN     | "nifi@EXAMPLE.COM"                                || "nifi/${InetAddress.localHost.canonicalHostName}@EXAMPLE.COM" as String
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_FQDN     | "nifi/node1.fakedomain.com" as String             || "nifi/node1.fakedomain.com"
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_FQDN     | "nifi/node1.fakedomain.com@EXAMPLE.COM" as String || "nifi/node1.fakedomain.com@EXAMPLE.COM" as String
        KeytabCredentialsService.PRINCIPAL_QUALIFIER_STRATEGY_FQDN     | "nifi/node1@EXAMPLE.COM" as String                || "nifi/node1@EXAMPLE.COM" as String
    }

    class KerberosCredentialsServiceProcessor extends AbstractProcessor {
        static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
                .name("kerberos-credentials-service")
                .displayName("Kerberos Credentials Service")
                .description("Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos")
                .identifiesControllerService(KerberosCredentialsService)
                .required(true)
                .build()
        def actualPrincipal = new AtomicReference<String>()

        @Override
        protected void init(ProcessorInitializationContext context) {
            super.init(context)
        }

        @Override
        void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
            def kcs = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService)
            actualPrincipal.set(kcs.principal)
        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return [KERBEROS_CREDENTIALS_SERVICE]
        }
    }
}
