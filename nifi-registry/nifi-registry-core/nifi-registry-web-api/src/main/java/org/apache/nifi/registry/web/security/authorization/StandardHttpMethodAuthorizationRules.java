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
package org.apache.nifi.registry.web.security.authorization;

import org.springframework.http.HttpMethod;

import java.util.EnumSet;
import java.util.Set;

public class StandardHttpMethodAuthorizationRules implements HttpMethodAuthorizationRules {

    final private Set<HttpMethod> methodsRequiringAuthorization;

    public StandardHttpMethodAuthorizationRules() {
        this(EnumSet.allOf(HttpMethod.class));
    }

    public StandardHttpMethodAuthorizationRules(Set<HttpMethod> methodsRequiringAuthorization) {
        this.methodsRequiringAuthorization = methodsRequiringAuthorization;
    }

    @Override
    public boolean requiresAuthorization(HttpMethod httpMethod) {
        return methodsRequiringAuthorization.contains(httpMethod);
    }
}
