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
package org.apache.nifi.atlas.security;

import org.apache.atlas.AtlasClientV2;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.util.StringUtils;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_PASSWORD;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_USER;

public class Basic implements AtlasAuthN {

    private String user;
    private String password;

    @Override
    public Collection<ValidationResult> validate(ValidationContext context) {
        return Stream.of(
                validateRequiredField(context, ATLAS_USER),
                validateRequiredField(context, ATLAS_PASSWORD)
        ).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
    }

    @Override
    public void configure(PropertyContext context) {
        user = context.getProperty(ATLAS_USER).evaluateAttributeExpressions().getValue();
        password = context.getProperty(ATLAS_PASSWORD).evaluateAttributeExpressions().getValue();

        if (StringUtils.isEmpty(user)) {
            throw new IllegalArgumentException("User is required for basic auth.");
        }

        if (StringUtils.isEmpty(password)){
            throw new IllegalArgumentException("Password is required for basic auth.");
        }
    }

    @Override
    public AtlasClientV2 createClient(String[] baseUrls) {
        return new AtlasClientV2(baseUrls, new String[]{user, password});
    }
}
