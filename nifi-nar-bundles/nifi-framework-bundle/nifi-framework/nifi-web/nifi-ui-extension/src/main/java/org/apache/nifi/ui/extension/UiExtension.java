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
package org.apache.nifi.ui.extension;

import org.apache.nifi.web.UiExtensionType;

/**
 * Information about a UI extension required to be invoked.
 */
public class UiExtension {

    private final UiExtensionType extensionType;
    private final String contextPath;

    public UiExtension(final UiExtensionType extensionType, final String contextPath) {
        this.extensionType = extensionType;
        this.contextPath = contextPath;
    }

    /**
     * @return type of this UI extension
     */
    public UiExtensionType getExtensionType() {
        return extensionType;
    }

    /**
     * @return The context path of this UI extension
     */
    public String getContextPath() {
        return contextPath;
    }

}
