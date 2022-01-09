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
import org.apache.nifi.registry.link.LinkableEntity;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Comparator;
import java.util.Objects;

@ApiModel
@XmlRootElement
public class ExtensionRepoGroup extends LinkableEntity implements Comparable<ExtensionRepoGroup> {

    private String bucketName;
    private String groupId;

    @ApiModelProperty(value = "The bucket name")
    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    @ApiModelProperty(value = "The group id")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Override
    public int compareTo(final ExtensionRepoGroup o) {
        return Comparator.comparing(ExtensionRepoGroup::getGroupId)
                .thenComparing(ExtensionRepoGroup::getBucketName)
                .compare(this, o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.bucketName, this.groupId) ;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final ExtensionRepoGroup other = (ExtensionRepoGroup) obj;

        return Objects.equals(this.getBucketName(), other.getBucketName())
                && Objects.equals(this.getGroupId(), other.getGroupId());
    }

}
