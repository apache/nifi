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
package org.apache.nifi.cef;

final class CEFSchemaInferenceBuilder {
    private String rawMessageField = null;
    private String invalidField = null;
    private boolean includeExtensions = false;
    private boolean includeCustomExtensions = false;
    private CEFCustomExtensionTypeResolver dataTypeResolver = CEFCustomExtensionTypeResolver.SKIPPING_RESOLVER;

    CEFSchemaInferenceBuilder withRawMessage(final String rawMessageField) {
        this.rawMessageField = rawMessageField;
        return this;
    }

    CEFSchemaInferenceBuilder withInvalidField(final String invalidField) {
        this.invalidField = invalidField;
        return this;
    }

    CEFSchemaInferenceBuilder withExtensions() {
        this.includeExtensions = true;
        return this;
    }

    CEFSchemaInferenceBuilder withCustomExtensions(final CEFCustomExtensionTypeResolver dataTypeResolver) {
        withExtensions();
        this.includeCustomExtensions = true;
        this.dataTypeResolver = dataTypeResolver;
        return this;
    }

    CEFSchemaInference build() {
        return new CEFSchemaInference(includeExtensions, includeCustomExtensions, dataTypeResolver, rawMessageField, invalidField);
    }
}
