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
package org.apache.nifi.registry.flow;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.registry.bucket.BucketItem;
import org.apache.nifi.registry.bucket.BucketItemType;
import org.apache.nifi.registry.revision.entity.RevisableEntity;
import org.apache.nifi.registry.revision.entity.RevisionInfo;

import javax.validation.constraints.Min;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Objects;

/**
 * <p>
 * Represents a versioned flow. A versioned flow is a named flow that is expected to change
 * over time. This flow is saved to the registry with information such as its name, a description,
 * and each version of the flow.
 * </p>
 *
 * @see VersionedFlowSnapshot
 */
@XmlRootElement
@ApiModel
public class VersionedFlow extends BucketItem implements RevisableEntity {

    @Min(0)
    private long versionCount;

    private RevisionInfo revision;

    public VersionedFlow() {
        super(BucketItemType.Flow);
    }

    @ApiModelProperty(value = "The number of versions of this flow.", readOnly = true)
    public long getVersionCount() {
        return versionCount;
    }

    public void setVersionCount(long versionCount) {
        this.versionCount = versionCount;
    }

    @ApiModelProperty(
            value = "The revision of this entity used for optimistic-locking during updates.",
            readOnly = true
    )
    @Override
    public RevisionInfo getRevision() {
        return revision;
    }

    @Override
    public void setRevision(RevisionInfo revision) {
        this.revision = revision;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.getIdentifier());
    }

}
