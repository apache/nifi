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

import java.util.List;

/**
 * Information about supported mime types for a Content Viewer.
 */
public class SupportedMimeTypes {

    private final String displayName;
    private final List<String> mimeTypes;

    public SupportedMimeTypes(final String displayName, final List<String> mimeTypes) {
        this.displayName = displayName;
        this.mimeTypes = mimeTypes;
    }

    /**
     * @return mime types supported by this content viewer
     */
    public List<String> getMimeTypes() {
        return mimeTypes;
    }

    /**
     * @return The context path of this UI extension
     */
    public String getDisplayName() {
        return displayName;
    }

}
