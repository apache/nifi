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
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.registry.link.LinkAdapter;

@XmlRootElement
public class ExtensionRepoVersion {

    private Link extensionsLink;
    private Link downloadLink;
    private Link sha256Link;
    private Boolean sha256Supplied;

    @XmlElement
    @XmlJavaTypeAdapter(LinkAdapter.class)
    @Schema(description = "The WebLink to view the metadata about the extensions contained in the extension bundle.",
            type = "org.apache.nifi.registry.link.JaxbLink", accessMode = Schema.AccessMode.READ_ONLY)
    public Link getExtensionsLink() {
        return extensionsLink;
    }

    public void setExtensionsLink(Link extensionsLink) {
        this.extensionsLink = extensionsLink;
    }

    @XmlElement
    @XmlJavaTypeAdapter(LinkAdapter.class)
    @Schema(description = "The WebLink to download this version of the extension bundle.",
            type = "org.apache.nifi.registry.link.JaxbLink", accessMode = Schema.AccessMode.READ_ONLY)
    public Link getDownloadLink() {
        return downloadLink;
    }

    public void setDownloadLink(Link downloadLink) {
        this.downloadLink = downloadLink;
    }

    @XmlElement
    @XmlJavaTypeAdapter(LinkAdapter.class)
    @Schema(description = "The WebLink to retrieve the SHA-256 digest for this version of the extension bundle.",
            type = "org.apache.nifi.registry.link.JaxbLink", accessMode = Schema.AccessMode.READ_ONLY)
    public Link getSha256Link() {
        return sha256Link;
    }

    public void setSha256Link(Link sha256Link) {
        this.sha256Link = sha256Link;
    }

    @Schema(description = "Indicates if the client supplied a SHA-256 when uploading this version of the extension bundle.",
        type = "org.apache.nifi.registry.link.JaxbLink", accessMode = Schema.AccessMode.READ_ONLY)
    public Boolean getSha256Supplied() {
        return sha256Supplied;
    }

    public void setSha256Supplied(Boolean sha256Supplied) {
        this.sha256Supplied = sha256Supplied;
    }
}
