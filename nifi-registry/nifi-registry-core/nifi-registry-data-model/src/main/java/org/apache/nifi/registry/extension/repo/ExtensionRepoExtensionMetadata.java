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
package org.apache.nifi.registry.extension.repo;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.ws.rs.core.Link;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.registry.extension.component.ExtensionMetadata;
import org.apache.nifi.registry.link.LinkAdapter;
import org.apache.nifi.registry.link.LinkableDocs;
import org.apache.nifi.registry.link.LinkableEntity;

import java.util.Comparator;

public class ExtensionRepoExtensionMetadata extends LinkableEntity implements LinkableDocs, Comparable<ExtensionRepoExtensionMetadata> {

    private ExtensionMetadata extensionMetadata;
    private Link linkDocs;

    public ExtensionRepoExtensionMetadata() {
    }

    public ExtensionRepoExtensionMetadata(final ExtensionMetadata extensionMetadata) {
        this.extensionMetadata = extensionMetadata;
    }

    @Schema(description = "The extension metadata")
    public ExtensionMetadata getExtensionMetadata() {
        return extensionMetadata;
    }

    public void setExtensionMetadata(ExtensionMetadata extensionMetadata) {
        this.extensionMetadata = extensionMetadata;
    }

    @Override
    @XmlElement
    @XmlJavaTypeAdapter(LinkAdapter.class)
    @Schema(description = "A WebLink to the documentation for this extension.",
            type = "org.apache.nifi.registry.link.JaxbLink", accessMode = Schema.AccessMode.READ_ONLY)
    public Link getLinkDocs() {
        return linkDocs;
    }

    @Override
    public void setLinkDocs(Link link) {
        this.linkDocs = link;
    }

    @Override
    public int compareTo(ExtensionRepoExtensionMetadata o) {
        return Comparator.comparing(ExtensionRepoExtensionMetadata::getExtensionMetadata).compare(this, o);
    }
}
