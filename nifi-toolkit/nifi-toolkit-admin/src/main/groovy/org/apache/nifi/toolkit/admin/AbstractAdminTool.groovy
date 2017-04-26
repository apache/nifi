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
import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.toolkit.admin.util.Version
import org.apache.nifi.util.StringUtils
import org.slf4j.Logger
import java.nio.file.Path
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

    Properties getBootstrapConf(Path bootstrapConfFileName) {
        Properties bootstrapProperties = new Properties()
        File bootstrapConf = bootstrapConfFileName.toFile()
        bootstrapProperties.load(new FileInputStream(bootstrapConf))
        return bootstrapProperties
    }

    String getRelativeDirectory(String directory, String rootDirectory) {
        if (directory.startsWith("./")) {
            final String directoryUpdated =  SystemUtils.IS_OS_WINDOWS ? File.separator + directory.substring(2,directory.length()) : directory.substring(1,directory.length())
            rootDirectory + directoryUpdated
        } else {
            directory
        }
    }

    Boolean supportedNiFiMinimumVersion(final String nifiConfDirName, final String nifiLibDirName, final String supportedMinimumVersion){
        final File nifiConfDir = new File(nifiConfDirName)
        final File nifiLibDir = new File (nifiLibDirName)
        final String versionStr = AdminUtil.getNiFiVersion(nifiConfDir,nifiLibDir)

        if(!StringUtils.isEmpty(versionStr)){
            Version version = new Version(versionStr.replace("-","."),".")
            Version minVersion = new Version(supportedMinimumVersion,".")
            Version.VERSION_COMPARATOR.compare(version,minVersion) >= 0
        }else{
            return false
        }

    }

    Boolean supportedNiFiMinimumVersion(final String nifiCurrentDirName, final String supportedMinimumVersion){
        final String bootstrapConfFileName = Paths.get(nifiCurrentDirName,"conf","bootstrap.conf").toString()
        final File bootstrapConf = new File(bootstrapConfFileName)
        final Properties bootstrapProperties = getBootstrapConf(Paths.get(bootstrapConfFileName))
        final String parentPathName = bootstrapConf.getCanonicalFile().getParentFile().getParentFile().getCanonicalPath()
        final String nifiConfDir = getRelativeDirectory(bootstrapProperties.getProperty("conf.dir"),parentPathName)
        final String nifiLibDir = getRelativeDirectory(bootstrapProperties.getProperty("lib.dir"),parentPathName)
        return supportedNiFiMinimumVersion(nifiConfDir,nifiLibDir,supportedMinimumVersion)
    }


}
