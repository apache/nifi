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

package org.apache.nifi.toolkit.admin.configmigrator

import groovy.xml.XmlUtil
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.SystemUtils
import org.junit.Rule
import org.junit.contrib.java.lang.system.SystemOutRule
import spock.lang.Specification
import org.junit.contrib.java.lang.system.ExpectedSystemExit

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission

class ConfigMigratorSpec extends Specification{

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none()

    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog()


    def "get rules directory name"(){

        setup:

        def config = new ConfigMigrator(false,false)
        def nifiVersion = "1.1.0"
        def nifiUpgradeVersion = "1.3.0"

        when:

        def rulesDirs = config.getRulesDirectoryName(nifiVersion,nifiUpgradeVersion)

        then:
        rulesDirs.size() == 2
        rulesDirs[0].endsWith("rules/v1_2_0")
        rulesDirs[1].endsWith("rules/v1_3_0")

    }

    def "get script rule name"(){

        setup:
        def config = new ConfigMigrator(false,false)
        def fileName = "flow.xml.gz"

        when:

        def script = config.getScriptRuleName(fileName)

        then:

        script == "flow-xml-gz.groovy"

    }

    def "parse argument and migrate property config successfully"(){

        setup:

        def File tmpDir = setupTmpDir()
        def config = new ConfigMigrator(true,false)
        def bootstrapFile = new File("src/test/resources/conf/bootstrap.conf")
        def upgradeConfDir = new File("src/test/resources/upgrade")
        def File workingFile = new File("target/tmp/upgrade")

        if(workingFile.exists()) {
            workingFile.delete()
        }

        FileUtils.copyDirectory(upgradeConfDir,workingFile)
        def Properties updatedProperties = new Properties()
        def Properties bootstrapProperties = new Properties()

        when:

        config.run("src/test/resources/",bootstrapFile.path,workingFile.path)
        updatedProperties.load(new FileInputStream(workingFile.path + "/conf/nifi.properties"))
        bootstrapProperties.load(new FileInputStream(workingFile.path + "/conf/bootstrap.conf"))

        then:
        updatedProperties.getProperty("nifi.cluster.node.protocol.port") == "8300"
        bootstrapProperties.getProperty("java.arg.2") == "-Xms512m"
        bootstrapProperties.getProperty("lib.dir") == "./lib"

        cleanup:

        tmpDir.deleteOnExit()

    }

    def "parse argument and move over configs due to no rules successfully"(){

        setup:

        def File tmpDir = setupTmpDir()
        def config = new ConfigMigrator(true,false)
        def bootstrapFile = new File("src/test/resources/conf/bootstrap.conf")
        def upgradeConfDir = new File("src/test/resources/no_rules")
        def File workingFile = new File("target/tmp/no_rules")

        if(workingFile.exists()) {
            workingFile.delete()
        }

        FileUtils.copyDirectory(upgradeConfDir,workingFile)
        def Properties updatedProperties = new Properties()
        def Properties bootstrapProperties = new Properties()

        when:

        config.run("src/test/resources/",bootstrapFile.path,workingFile.path)
        updatedProperties.load(new FileInputStream(workingFile.path + "/conf/nifi.properties"))
        bootstrapProperties.load(new FileInputStream(workingFile.path + "/conf/bootstrap.conf"))

        then:
        updatedProperties.getProperty("nifi.cluster.node.protocol.port") == "8300"
        updatedProperties.getProperty("nifi.cluster.is.node") == "true"
        bootstrapProperties.getProperty("java.arg.1")

        cleanup:

        tmpDir.deleteOnExit()

    }

    def "parse arguments and migrate property config successfully with override"(){

        setup:

        def File tmpDir = setupTmpDir()
        def config = new ConfigMigrator(true,true)
        def nifiConfDir = new File("src/test/resources/conf")
        def nifiLibDir = new File("src/test/resources/lib")
        def externalConfDir = new File("src/test/resources/external/conf")
        def upgradeConfDir = new File("src/test/resources/upgrade")

        def File workingFile = new File("target/tmp/conf")
        def File workingLibFile = new File("target/tmp/lib")
        def File externalWorkingFile = new File("target/tmp/external/conf")
        def File upgradeWorkingFile = new File("target/tmp/upgrade")


        if(workingFile.exists()) {
            workingFile.delete()
        }

        if(externalWorkingFile.exists()){
            externalWorkingFile.delete()
        }

        if(upgradeWorkingFile.exists()){
            upgradeWorkingFile.delete()
        }

        FileUtils.copyDirectory(nifiConfDir,workingFile)
        FileUtils.copyDirectory(nifiLibDir,workingLibFile)
        FileUtils.copyDirectory(externalConfDir,externalWorkingFile)
        FileUtils.copyDirectory(upgradeConfDir,upgradeWorkingFile)

        def bootstrapFile = new File("target/tmp/external/conf/bootstrap.conf")
        def Properties updatedNiFiProperties = new Properties()
        def Properties updatedBootstrapProperties = new Properties()
        def File updatedLoginProvidersFile
        def xml

        when:
        config.run("target/tmp/external",bootstrapFile.path,upgradeWorkingFile.path)
        updatedNiFiProperties.load(new FileInputStream(workingFile.path + "/nifi.properties"))
        updatedBootstrapProperties.load(new FileInputStream(upgradeWorkingFile.path + "/conf/bootstrap.conf"))
        updatedLoginProvidersFile = new File(workingFile.path + "/login-identity-providers.xml")
        xml = new XmlSlurper().parse(updatedLoginProvidersFile)


        then:
        updatedNiFiProperties.getProperty("nifi.cluster.node.protocol.port") == "8300"
        updatedBootstrapProperties.getProperty("java.arg.2") == "-Xms512m"
        updatedBootstrapProperties.getProperty("lib.dir") == "./lib"
        xml.depthFirst().findAll { it.name() == "fake"}.size() == 1

        cleanup:

        tmpDir.deleteOnExit()

    }

    def setFilePermissions(File file, List<PosixFilePermission> permissions = []) {
        if (SystemUtils.IS_OS_WINDOWS) {
            file?.setReadable(permissions.contains(PosixFilePermission.OWNER_READ))
            file?.setWritable(permissions.contains(PosixFilePermission.OWNER_WRITE))
            file?.setExecutable(permissions.contains(PosixFilePermission.OWNER_EXECUTE))
        } else {
            Files.setPosixFilePermissions(file?.toPath(), permissions as Set)
        }
    }

    def setupTmpDir(String tmpDirPath = "target/tmp/") {
        File tmpDir = new File(tmpDirPath)
        tmpDir.mkdirs()
        setFilePermissions(tmpDir, [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
                                    PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE,
                                    PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE])
        tmpDir
    }

}
