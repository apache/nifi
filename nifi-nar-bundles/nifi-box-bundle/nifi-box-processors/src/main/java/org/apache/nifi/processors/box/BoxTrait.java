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
package org.apache.nifi.processors.box;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxConfig;
import com.box.sdk.BoxDeveloperEditionAPIConnection;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public interface BoxTrait {
    PropertyDescriptor USER_ID = new PropertyDescriptor.Builder()
        .name("box-user-id")
        .displayName("User ID")
        .description("The ID of the user who owns the listed folder.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .required(true)
        .build();

    PropertyDescriptor BOX_CONFIG_FILE = new PropertyDescriptor.Builder()
        .name("box-config-file")
        .displayName("Box Config File")
        .description("Full path of the Box config JSON file. See Processor's Usage / Additional Details for more information.")
        .required(true)
        .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
        .build();

    default BoxAPIConnection createBoxApiConnection(
        ProcessContext context,
        ProxyConfiguration proxyConfiguration
    ) {
        BoxAPIConnection api;

        String userId = context.getProperty(USER_ID).evaluateAttributeExpressions().getValue();
        String boxConfigFile = context.getProperty(BOX_CONFIG_FILE).getValue();

        try (
            Reader reader = new FileReader(boxConfigFile)
        ) {
            BoxConfig boxConfig = BoxConfig.readFrom(reader);

            api = BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection(boxConfig);
            api.setProxy(proxyConfiguration.createProxy());

            api.asUser(userId);
        } catch (FileNotFoundException e) {
            throw new ProcessException("Couldn't find Box config file", e);
        } catch (IOException e) {
            throw new ProcessException("Couldn't read Box config file", e);
        }

        return api;
    }
}
