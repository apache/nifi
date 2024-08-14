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
package org.apache.nifi.ui.extension.contentviewer;

import org.apache.nifi.bundle.Bundle;

import java.util.List;
import java.util.Objects;

/**
 * Information about a Content Viewer.
 */
public class ContentViewer {

    private final String contextPath;
    private final List<SupportedMimeTypes> supportedMimeTypes;
    private final Bundle bundle;

    public ContentViewer(final String contextPath, final List<SupportedMimeTypes> supportedMimeTypes, final Bundle bundle) {
        this.contextPath = contextPath;
        this.supportedMimeTypes = supportedMimeTypes;
        this.bundle = bundle;
    }

    /**
     * @return mime types supported by this content viewer
     */
    public List<SupportedMimeTypes> getSupportedMimeTypes() {
        return supportedMimeTypes;
    }

    /**
     * @return The context path of this UI extension
     */
    public String getContextPath() {
        return contextPath;
    }

    /**
     * @return The bundle for this content viewer
     */
    public Bundle getBundle() {
        return bundle;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContentViewer that = (ContentViewer) o;
        return Objects.equals(contextPath, that.contextPath);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(contextPath);
    }
}
