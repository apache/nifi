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
package org.apache.nifi.registry.extension.component;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.registry.extension.bundle.BundleInfo;
import org.apache.nifi.registry.extension.component.manifest.DeprecationNotice;
import org.apache.nifi.registry.extension.component.manifest.ExtensionType;
import org.apache.nifi.registry.extension.component.manifest.ProvidedServiceAPI;
import org.apache.nifi.registry.extension.component.manifest.Restricted;
import org.apache.nifi.registry.link.LinkAdapter;
import org.apache.nifi.registry.link.LinkableDocs;
import org.apache.nifi.registry.link.LinkableEntity;

import javax.ws.rs.core.Link;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@ApiModel
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

    @ApiModelProperty(value = "The name of the extension")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty(value = "The display name of the extension")
    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @ApiModelProperty(value = "The type of the extension")
    public ExtensionType getType() {
        return type;
    }

    public void setType(ExtensionType type) {
        this.type = type;
    }

    @ApiModelProperty(value = "The description of the extension")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @ApiModelProperty(value = "The deprecation notice of the extension")
    public DeprecationNotice getDeprecationNotice() {
        return deprecationNotice;
    }

    public void setDeprecationNotice(DeprecationNotice deprecationNotice) {
        this.deprecationNotice = deprecationNotice;
    }

    @ApiModelProperty(value = "The tags of the extension")
    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    @ApiModelProperty(value = "The restrictions of the extension")
    public Restricted getRestricted() {
        return restricted;
    }

    public void setRestricted(Restricted restricted) {
        this.restricted = restricted;
    }

    @ApiModelProperty(value = "The service APIs provided by the extension")
    public List<ProvidedServiceAPI> getProvidedServiceAPIs() {
        return providedServiceAPIs;
    }

    public void setProvidedServiceAPIs(List<ProvidedServiceAPI> providedServiceAPIs) {
        this.providedServiceAPIs = providedServiceAPIs;
    }

    @ApiModelProperty(value = "The information for the bundle where this extension is located")
    public BundleInfo getBundleInfo() {
        return bundleInfo;
    }

    public void setBundleInfo(BundleInfo bundleInfo) {
        this.bundleInfo = bundleInfo;
    }

    @ApiModelProperty(value = "Whether or not the extension has additional detail documentation")
    public boolean getHasAdditionalDetails() {
        return hasAdditionalDetails;
    }

    public void setHasAdditionalDetails(boolean hasAdditionalDetails) {
        this.hasAdditionalDetails = hasAdditionalDetails;
    }

    @Override
    @XmlElement
    @XmlJavaTypeAdapter(LinkAdapter.class)
    @ApiModelProperty(value = "A WebLink to the documentation for this extension.",
            dataType = "org.apache.nifi.registry.link.JaxbLink", readOnly = true)
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
