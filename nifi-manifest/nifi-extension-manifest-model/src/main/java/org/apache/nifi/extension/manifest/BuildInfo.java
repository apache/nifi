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
package org.apache.nifi.extension.manifest;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

@ApiModel
@XmlAccessorType(XmlAccessType.FIELD)
public class BuildInfo {

    private String tag;
    private String branch;
    private String revision;
    private String jdk;
    private String builtBy;
    private String timestamp;

    @ApiModelProperty(value = "The tag the NAR was built from")
    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    @ApiModelProperty(value = "The branch the NAR was built from")
    public String getBranch() {
        return branch;
    }

    public void setBranch(String branch) {
        this.branch = branch;
    }

    @ApiModelProperty(value = "The revision the NAR was built from")
    public String getRevision() {
        return revision;
    }

    public void setRevision(String revision) {
        this.revision = revision;
    }

    @ApiModelProperty(value = "The JDK the NAR was built with")
    public String getJdk() {
        return jdk;
    }

    public void setJdk(String jdk) {
        this.jdk = jdk;
    }

    @ApiModelProperty(value = "The OS user that performed the build")
    public String getBuiltBy() {
        return builtBy;
    }

    public void setBuiltBy(String builtBy) {
        this.builtBy = builtBy;
    }

    @ApiModelProperty(value = "The timestamp of the build")
    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
