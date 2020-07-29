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
package org.apache.nifi.toolkit.admin

import org.apache.nifi.toolkit.admin.util.AdminUtil
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.slf4j.Logger
import java.nio.file.Paths

public abstract class AbstractAdminTool {

    protected static final String JAVA_HOME = "JAVA_HOME"
    protected static final String NIFI_TOOLKIT_HOME = "NIFI_TOOLKIT_HOME"
    protected static final String SEP = System.lineSeparator()
    protected Options options
    protected String header
    protected String footer
    protected Boolean isVerbose
    protected Logger logger

    protected void setup(){
        options = getOptions()
        footer = buildFooter()
        logger = getLogger()
    }

    protected String buildHeader(final String description ) {
        "${SEP}${description}${SEP * 2}"
    }

    protected String buildFooter() {
        "${SEP}Java home: ${System.getenv(JAVA_HOME)}${SEP}NiFi Toolkit home: ${System.getenv(NIFI_TOOLKIT_HOME)}"
    }

    public void printUsage(final String errorMessage) {
        if (errorMessage) {
            System.out.println(errorMessage)
            System.out.println()
        }
        final HelpFormatter helpFormatter = new HelpFormatter()
        helpFormatter.setWidth(160)
        helpFormatter.printHelp(this.class.getCanonicalName(), this.header, options, footer, true)
    }

    protected abstract Options getOptions()

    protected abstract Logger getLogger()

    Boolean supportedNiFiMinimumVersion(final String nifiCurrentDirName, final String bootstrapConfFileName, final String supportedMinimumVersion){
        final Properties bootstrapProperties = AdminUtil.getBootstrapConf(Paths.get(bootstrapConfFileName))
        final String nifiConfDir = AdminUtil.getRelativeDirectory(bootstrapProperties.getProperty("conf.dir"),nifiCurrentDirName)
        final String nifiLibDir = AdminUtil.getRelativeDirectory(bootstrapProperties.getProperty("lib.dir"),nifiCurrentDirName)
        return AdminUtil.supportedNiFiMinimumVersion(nifiConfDir,nifiLibDir,supportedMinimumVersion)
    }


}
