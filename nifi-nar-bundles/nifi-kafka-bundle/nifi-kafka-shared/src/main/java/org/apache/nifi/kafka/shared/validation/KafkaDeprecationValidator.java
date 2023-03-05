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

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.deprecation.log.DeprecationLogger;
import org.apache.nifi.deprecation.log.DeprecationLoggerFactory;

import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.KERBEROS_CREDENTIALS_SERVICE;
import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.KERBEROS_KEYTAB;
import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.KERBEROS_PRINCIPAL;
import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.SELF_CONTAINED_KERBEROS_USER_SERVICE;

public class KafkaDeprecationValidator {

    public static void validate(Class<?> componentClass, final String identifier, final ValidationContext validationContext) {
        final DeprecationLogger deprecationLogger = DeprecationLoggerFactory.getLogger(componentClass);

        final PropertyValue credentialsServiceProperty = validationContext.getProperty(KERBEROS_CREDENTIALS_SERVICE);
        final PropertyValue principalProperty = validationContext.getProperty(KERBEROS_PRINCIPAL).evaluateAttributeExpressions();
        final PropertyValue keyTabProperty = validationContext.getProperty(KERBEROS_KEYTAB).evaluateAttributeExpressions();

        if (credentialsServiceProperty.isSet()) {
            deprecationLogger.warn("{}[id={}] [{}] Property should be replaced with [{}] Property",
                    componentClass.getSimpleName(),
                    identifier,
                    KERBEROS_CREDENTIALS_SERVICE.getDisplayName(),
                    SELF_CONTAINED_KERBEROS_USER_SERVICE.getDisplayName()
            );
        }

        if (principalProperty.isSet() || keyTabProperty.isSet()) {
            deprecationLogger.warn("{}[id={}] [{}] and [{}] Properties should be replaced with [{}] Property",
                    componentClass.getSimpleName(),
                    identifier,
                    KERBEROS_PRINCIPAL.getDisplayName(),
                    KERBEROS_KEYTAB.getDisplayName(),
                    SELF_CONTAINED_KERBEROS_USER_SERVICE.getDisplayName()
            );
        }
    }
}
