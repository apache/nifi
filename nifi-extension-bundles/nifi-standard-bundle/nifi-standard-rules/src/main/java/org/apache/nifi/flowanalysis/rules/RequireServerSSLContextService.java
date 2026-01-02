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
package org.apache.nifi.flowanalysis.rules;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.ComponentAnalysisResult;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.util.StringUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

@Tags({"component", "processor", "controller service", "type", "ssl", "tls", "listen"})
@CapabilityDescription("Produces rule violations for each component (i.e. processors or controller services) having a property "
        + "identifying an SSLContextService that is not set.")
@UseCase(
        description = "Ensure that an SSL Context has been configured for the specified components. This helps avoid ports being opened for insecure (plaintext, e.g.) communications.",
        configuration = """
                To avoid the violation, ensure that the "SSL Context Service" property is set for the specified component(s).
                """
)
public class RequireServerSSLContextService extends AbstractFlowAnalysisRule {

    private static final List<String> COMPONENT_TYPES = List.of(
            "org.apache.nifi.processors.standard.ListenFTP",
            "org.apache.nifi.processors.standard.ListenHTTP",
            "org.apache.nifi.processors.standard.ListenTCP",
            "org.apache.nifi.processors.standard.ListenSyslog",
            "org.apache.nifi.processors.standard.HandleHttpRequest",
            "org.apache.nifi.websocket.jetty.JettyWebSocketServer"
    );

    private static final String SERVICE_PROPERTY_NAME = "SSL Context Service";

    @Override
    public Collection<ComponentAnalysisResult> analyzeComponent(VersionedComponent component, FlowAnalysisRuleContext context) {
        final Collection<ComponentAnalysisResult> results = new HashSet<>();

        if (component instanceof VersionedConfigurableExtension versionedConfigurableExtension) {
            final String encounteredComponentType = versionedConfigurableExtension.getType();

            if (COMPONENT_TYPES.contains(encounteredComponentType)) {
                // Loop over the properties for this component looking for an SSLContextService
                versionedConfigurableExtension.getProperties().forEach((propertyName, propertyValue) -> {

                    // If the SSL Context property exists and the value is not set, report a violation
                    if (SERVICE_PROPERTY_NAME.equals(propertyName) && StringUtils.isEmpty(propertyValue)) {
                        final String encounteredSimpleComponentType = encounteredComponentType.substring(encounteredComponentType.lastIndexOf(".") + 1);
                        final ComponentAnalysisResult result = new ComponentAnalysisResult(
                                component.getInstanceIdentifier(),
                                "'" + encounteredSimpleComponentType + "' must specify an SSL Context Service"
                        );

                        results.add(result);

                    }
                });
            }
        }
        return results;
    }
}
