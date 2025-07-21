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

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Set;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlTransient;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.link.LinkableEntity;

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


    @Schema(description = "The metadata about this version of the extension bundle")
    public BundleVersionMetadata getVersionMetadata() {
        return versionMetadata;
    }

    public void setVersionMetadata(BundleVersionMetadata versionMetadata) {
        this.versionMetadata = versionMetadata;
    }

    @Schema(description = "The set of other bundle versions that this version is dependent on", accessMode = Schema.AccessMode.READ_ONLY)
    public Set<BundleVersionDependency> getDependencies() {
        return dependencies;
    }

    public void setDependencies(Set<BundleVersionDependency> dependencies) {
        this.dependencies = dependencies;
    }

    @Schema(description = "The bundle this version is for", accessMode = Schema.AccessMode.READ_ONLY)
    public Bundle getBundle() {
        return bundle;
    }

    public void setBundle(Bundle bundle) {
        this.bundle = bundle;
    }

    @Schema(description = "The bucket that the extension bundle belongs to")
    public Bucket getBucket() {
        return bucket;
    }

    public void setBucket(Bucket bucket) {
        this.bucket = bucket;
    }

    @XmlTransient
    public String getFilename() {
        final String filename = bundle.getArtifactId() + "-" + versionMetadata.getVersion();

        return switch (bundle.getBundleType()) {
            case NIFI_NAR -> filename + ".nar";
            case MINIFI_CPP ->
                // TODO should CPP get a special extension
                    filename;
        };
    }

}
