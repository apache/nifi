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

@ApiModel
public class BuildInfo {

    private String buildTool;

    private String buildFlags;

    private String buildBranch;

    private String buildTag;

    private String buildRevision;

    private long built;

    private String builtBy;

    @ApiModelProperty(value = "The tool used to build the version of the bundle")
    public String getBuildTool() {
        return buildTool;
    }

    public void setBuildTool(String buildTool) {
        this.buildTool = buildTool;
    }

    @ApiModelProperty(value = "The flags used to build the version of the bundle")
    public String getBuildFlags() {
        return buildFlags;
    }

    public void setBuildFlags(String buildFlags) {
        this.buildFlags = buildFlags;
    }

    @ApiModelProperty(value = "The branch used to build the version of the bundle")
    public String getBuildBranch() {
        return buildBranch;
    }

    public void setBuildBranch(String buildBranch) {
        this.buildBranch = buildBranch;
    }

    @ApiModelProperty(value = "The tag used to build the version of the bundle")
    public String getBuildTag() {
        return buildTag;
    }

    public void setBuildTag(String buildTag) {
        this.buildTag = buildTag;
    }

    @ApiModelProperty(value = "The revision used to build the version of the bundle")
    public String getBuildRevision() {
        return buildRevision;
    }

    public void setBuildRevision(String buildRevision) {
        this.buildRevision = buildRevision;
    }

    @ApiModelProperty(value = "The timestamp the version of the bundle was built")
    public long getBuilt() {
        return built;
    }

    public void setBuilt(long built) {
        this.built = built;
    }

    @ApiModelProperty(value = "The identity of the user that performed the build")
    public String getBuiltBy() {
        return builtBy;
    }

    public void setBuiltBy(String builtBy) {
        this.builtBy = builtBy;
    }

}
