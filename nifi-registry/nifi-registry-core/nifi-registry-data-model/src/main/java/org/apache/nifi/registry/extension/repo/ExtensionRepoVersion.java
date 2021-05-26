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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.registry.link.LinkAdapter;

import javax.ws.rs.core.Link;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

@ApiModel
@XmlRootElement
public class ExtensionRepoVersion {

    private Link extensionsLink;
    private Link downloadLink;
    private Link sha256Link;
    private Boolean sha256Supplied;

    @XmlElement
    @XmlJavaTypeAdapter(LinkAdapter.class)
    @ApiModelProperty(value = "The WebLink to view the metadata about the extensions contained in the extension bundle.",
            dataType = "org.apache.nifi.registry.link.JaxbLink", readOnly = true)
    public Link getExtensionsLink() {
        return extensionsLink;
    }

    public void setExtensionsLink(Link extensionsLink) {
        this.extensionsLink = extensionsLink;
    }

    @XmlElement
    @XmlJavaTypeAdapter(LinkAdapter.class)
    @ApiModelProperty(value = "The WebLink to download this version of the extension bundle.",
            dataType = "org.apache.nifi.registry.link.JaxbLink", readOnly = true)
    public Link getDownloadLink() {
        return downloadLink;
    }

    public void setDownloadLink(Link downloadLink) {
        this.downloadLink = downloadLink;
    }

    @XmlElement
    @XmlJavaTypeAdapter(LinkAdapter.class)
    @ApiModelProperty(value = "The WebLink to retrieve the SHA-256 digest for this version of the extension bundle.",
            dataType = "org.apache.nifi.registry.link.JaxbLink", readOnly = true)
    public Link getSha256Link() {
        return sha256Link;
    }

    public void setSha256Link(Link sha256Link) {
        this.sha256Link = sha256Link;
    }

    @ApiModelProperty(value = "Indicates if the client supplied a SHA-256 when uploading this version of the extension bundle.",
            dataType = "org.apache.nifi.registry.link.JaxbLink", readOnly = true)
    public Boolean getSha256Supplied() {
        return sha256Supplied;
    }

    public void setSha256Supplied(Boolean sha256Supplied) {
        this.sha256Supplied = sha256Supplied;
    }
}
