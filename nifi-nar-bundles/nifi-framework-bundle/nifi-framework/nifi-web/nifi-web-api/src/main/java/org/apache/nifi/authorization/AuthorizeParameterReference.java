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
package org.apache.nifi.authorization;

import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.parameter.ExpressionLanguageAgnosticParameterParser;
import org.apache.nifi.parameter.ParameterParser;
import org.apache.nifi.parameter.ParameterTokenList;

import java.util.Map;

public class AuthorizeParameterReference {

    public static void authorizeParameterReferences(final Map<String, String> proposedProperties, final Map<String, String> currentProperties, final Authorizer authorizer,
                                                    final Authorizable parameterContextAuthorizable, final NiFiUser user) {
        if (proposedProperties == null) {
            return;
        }

        final ParameterParser parameterParser = new ExpressionLanguageAgnosticParameterParser();

        boolean referencesParameter = false;
        for (final Map.Entry<String, String> entry : proposedProperties.entrySet()) {
            final String propertyName = entry.getKey();
            final String proposedPropertyValue = entry.getValue();

            // Check if any Parameter is referenced. If so, user must have READ policy on the Parameter Context
            ParameterTokenList tokenList = parameterParser.parseTokens(proposedPropertyValue);
            if (!tokenList.toReferenceList().isEmpty()) {
                referencesParameter = true;
                break;
            }

            // If the proposed value does not reference a Parameter but the old value does (the Parameter is being de-referenced) then we must also ensure that
            // the user has the READ policy on the Parameter Context. This is consistent with our policies on referencing Controller Services. This is done largely
            // to ensure that if a user does not have the READ policy they are not able to change a value so that it doesn't reference the Parameter and then be left
            // in a state where they cannot undo that change because they cannot change the value back to the previous value (as doing so would require READ policy
            // on the Parameter Context as per above)
            final String currentValue = currentProperties.get(propertyName);
            tokenList = parameterParser.parseTokens(currentValue);
            if (!tokenList.toReferenceList().isEmpty()) {
                referencesParameter = true;
                break;
            }
        }

        if (referencesParameter) {
            parameterContextAuthorizable.authorize(authorizer, RequestAction.READ, user);
        }
    }
}
