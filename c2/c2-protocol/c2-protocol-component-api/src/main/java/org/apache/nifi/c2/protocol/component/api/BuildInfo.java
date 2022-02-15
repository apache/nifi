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

package org.apache.nifi.c2.protocol.component.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;

@ApiModel
public class BuildInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private String version;
    private String revision;
    private Long timestamp;
    private String targetArch;
    private String compiler;
    private String compilerFlags;

    @ApiModelProperty("The version number of the built component.")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @ApiModelProperty("The SCM revision id of the source code used for this build.")
    public String getRevision() {
        return revision;
    }

    public void setRevision(String revision) {
        this.revision = revision;
    }

    @ApiModelProperty("The timestamp (milliseconds since Epoch) of the build.")
    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @ApiModelProperty("The target architecture of the built component.")
    public String getTargetArch() {
        return targetArch;
    }

    public void setTargetArch(String targetArch) {
        this.targetArch = targetArch;
    }

    @ApiModelProperty("The compiler used for the build")
    public String getCompiler() {
        return compiler;
    }

    public void setCompiler(String compiler) {
        this.compiler = compiler;
    }

    @ApiModelProperty("The compiler flags used for the build.")
    public String getCompilerFlags() {
        return compilerFlags;
    }

    public void setCompilerFlags(String compilerFlags) {
        this.compilerFlags = compilerFlags;
    }

    @Override
    public String toString() {
        return "BuildInfo{" +
                "version='" + version + '\'' +
                ", revision='" + revision + '\'' +
                ", timestamp=" + timestamp +
                ", targetArch='" + targetArch + '\'' +
                ", compiler='" + compiler + '\'' +
                ", compilerFlags='" + compilerFlags + '\'' +
                '}';
    }
}
