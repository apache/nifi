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
package org.apache.nifi.registry.extension.bundle;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.link.LinkableEntity;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import java.util.Set;

@ApiModel
@XmlRootElement
public class BundleVersion extends LinkableEntity {

    @Valid
    @NotNull
    private BundleVersionMetadata versionMetadata;

    // read-only, only populated from retrieval of an individual bundle version
    private Set<BundleVersionDependency> dependencies;

    // read-only, only populated from retrieval of an individual bundle version
    private Bundle bundle;

    // read-only, only populated from retrieval of an individual bundle version
    private Bucket bucket;


    @ApiModelProperty(value = "The metadata about this version of the extension bundle")
    public BundleVersionMetadata getVersionMetadata() {
        return versionMetadata;
    }

    public void setVersionMetadata(BundleVersionMetadata versionMetadata) {
        this.versionMetadata = versionMetadata;
    }

    @ApiModelProperty(value = "The set of other bundle versions that this version is dependent on", readOnly = true)
    public Set<BundleVersionDependency> getDependencies() {
        return dependencies;
    }

    public void setDependencies(Set<BundleVersionDependency> dependencies) {
        this.dependencies = dependencies;
    }

    @ApiModelProperty(value = "The bundle this version is for", readOnly = true)
    public Bundle getBundle() {
        return bundle;
    }

    public void setBundle(Bundle bundle) {
        this.bundle = bundle;
    }

    @ApiModelProperty(value = "The bucket that the extension bundle belongs to")
    public Bucket getBucket() {
        return bucket;
    }

    public void setBucket(Bucket bucket) {
        this.bucket = bucket;
    }

    @XmlTransient
    public String getFilename() {
        final String filename = bundle.getArtifactId() + "-" + versionMetadata.getVersion();

        switch (bundle.getBundleType()) {
            case NIFI_NAR:
                return filename + ".nar";
            case MINIFI_CPP:
                // TODO should CPP get a special extension
                return filename;
            default:
                throw new IllegalStateException("Unknown bundle type: " + bundle.getBundleType());
        }
    }

}
