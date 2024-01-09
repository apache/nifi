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
package org.apache.nifi.extension;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import jakarta.ws.rs.core.Link;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.extension.manifest.DeprecationNotice;
import org.apache.nifi.extension.manifest.ExtensionType;
import org.apache.nifi.extension.manifest.ProvidedServiceAPI;
import org.apache.nifi.extension.manifest.Restricted;
import org.apache.nifi.registry.extension.bundle.BundleInfo;
import org.apache.nifi.registry.link.LinkAdapter;
import org.apache.nifi.registry.link.LinkableDocs;
import org.apache.nifi.registry.link.LinkableEntity;

public class ExtensionMetadata extends LinkableEntity implements LinkableDocs, Comparable<ExtensionMetadata> {

    private String name;
    private String displayName;
    private ExtensionType type;
    private String description;
    private DeprecationNotice deprecationNotice;
    private List<String> tags;
    private Restricted restricted;
    private List<ProvidedServiceAPI> providedServiceAPIs;
    private BundleInfo bundleInfo;
    private boolean hasAdditionalDetails;
    private Link linkDocs;

    @Schema(description = "The name of the extension")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Schema(description = "The display name of the extension")
    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Schema(description = "The type of the extension")
    public ExtensionType getType() {
        return type;
    }

    public void setType(ExtensionType type) {
        this.type = type;
    }

    @Schema(description = "The description of the extension")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Schema(description = "The deprecation notice of the extension")
    public DeprecationNotice getDeprecationNotice() {
        return deprecationNotice;
    }

    public void setDeprecationNotice(DeprecationNotice deprecationNotice) {
        this.deprecationNotice = deprecationNotice;
    }

    @Schema(description = "The tags of the extension")
    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    @Schema(description = "The restrictions of the extension")
    public Restricted getRestricted() {
        return restricted;
    }

    public void setRestricted(Restricted restricted) {
        this.restricted = restricted;
    }

    @Schema(description = "The service APIs provided by the extension")
    public List<ProvidedServiceAPI> getProvidedServiceAPIs() {
        return providedServiceAPIs;
    }

    public void setProvidedServiceAPIs(List<ProvidedServiceAPI> providedServiceAPIs) {
        this.providedServiceAPIs = providedServiceAPIs;
    }

    @Schema(description = "The information for the bundle where this extension is located")
    public BundleInfo getBundleInfo() {
        return bundleInfo;
    }

    public void setBundleInfo(BundleInfo bundleInfo) {
        this.bundleInfo = bundleInfo;
    }

    @Schema(description = "Whether or not the extension has additional detail documentation")
    public boolean getHasAdditionalDetails() {
        return hasAdditionalDetails;
    }

    public void setHasAdditionalDetails(boolean hasAdditionalDetails) {
        this.hasAdditionalDetails = hasAdditionalDetails;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtensionMetadata extension = (ExtensionMetadata) o;
        return Objects.equals(name, extension.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public int compareTo(final ExtensionMetadata o) {
        return Comparator.comparing(ExtensionMetadata::getDisplayName).compare(this, o);
    }

}
