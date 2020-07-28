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

package org.apache.nifi.hbase;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class VisibilityLabelUtils {
    static final PropertyDescriptor AUTHORIZATIONS = new PropertyDescriptor.Builder()
        .name("hb-lu-authorizations")
        .displayName("Authorizations")
        .description("The list of authorization tokens to be used with cell visibility if it is enabled. These will be used to " +
                "override the default authorization list for the user accessing HBase.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static List<String> getAuthorizations(ConfigurationContext context) {
        List<String> tokens = new ArrayList<>();
        String authorizationString = context.getProperty(AUTHORIZATIONS).isSet()
                ? context.getProperty(AUTHORIZATIONS).getValue()
                : "";
        if (!StringUtils.isEmpty(authorizationString)) {
            tokens = Arrays.asList(authorizationString.split(",[\\s]*"));
        }

        return tokens;
    }
}
