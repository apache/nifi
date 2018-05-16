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

import com.google.common.collect.Lists
import com.google.common.io.Files
import org.apache.nifi.toolkit.admin.util.AdminUtil
import org.apache.commons.cli.ParseException
import org.apache.commons.io.FileUtils
import org.apache.nifi.toolkit.admin.util.Version
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Path
import java.nio.file.Paths

public class ConfigMigrator {

    private final static String SUPPORTED_MINIMUM_VERSION = '1.0.0'
    private final String RULES_DIR = getRulesDirectory()
    private final Boolean overwrite
    protected Logger logger =  LoggerFactory.getLogger(ConfigMigrator)
    protected final Boolean isVerbose

    public ConfigMigrator(Boolean verbose, Boolean overwrite) {
        this.overwrite = overwrite
        this.isVerbose = verbose
    }

    String getRulesDirectory() {
        final ClassLoader cl = this.getClass().getClassLoader()
        cl.getResource('rules').path.replaceAll('%20',' ')
    }

    List<String> getRulesDirectoryName(final String currentVersion, final String upgradeVersion) {
        Version current = new Version(currentVersion.take(5).toString(),'.')
        Version upgrade = new Version(upgradeVersion.take(5).toString(),'.')
        File rulesDir = new File(rulesDirectory)
        List<File> rules = Lists.newArrayList(rulesDir.listFiles())
        List<Version> versions = rules.collect { new Version(it.name[1..-1],'_')}
        versions.sort(Version.VERSION_COMPARATOR)
        List<Version> matches = versions.findAll { Version.VERSION_COMPARATOR.compare(it,upgrade) <= 0 && Version.VERSION_COMPARATOR.compare(it,current) == 1}

        if(matches.isEmpty()){
            null
        }else{
            matches.sort(Version.VERSION_COMPARATOR)
            List<String> directoryNames = []
            matches.each { directoryNames.add(RULES_DIR + File.separator + 'v' + it.toString()) }
            directoryNames
        }
    }

    Boolean supportedVersion(final File script, final String currentVersion) {
        final Class ruleClass = new GroovyClassLoader(getClass().getClassLoader()).parseClass(script)
        final GroovyObject ruleObject = (GroovyObject) ruleClass.newInstance()
        ruleObject.invokeMethod('supportedVersion', [currentVersion])
    }

    byte[] migrateContent(final File script, final byte[] content, final byte[] upgradeContent) {
        final Class ruleClass = new GroovyClassLoader(getClass().getClassLoader()).parseClass(script)
        final GroovyObject ruleObject = (GroovyObject) ruleClass.newInstance()
        ruleObject.invokeMethod('migrate', [content, upgradeContent])
    }

    String getScriptRuleName(final String fileName) {
        fileName.replace('.', '-') + '.groovy'
    }

    File getUpgradeFile(final File upgradeDir, final String fileName){

        final File[] upgradeFiles = upgradeDir.listFiles({dir, name -> name == fileName }as FilenameFilter)
        upgradeFiles.size() == 1 ? upgradeFiles[0] : new File(upgradeDir.path + File.separator + fileName)
    }

    void migrate(final File nifiConfDir, final File nifiLibDir, final File nifiUpgradeConfigDir, final File nifiUpgradeLibDir, final File boostrapConf, final String nifiCurrentDir) {

        final String nifiCurrentVersion = AdminUtil.getNiFiVersion(nifiConfDir,nifiLibDir)
        final String nifiUpgradeVersion = AdminUtil.getNiFiVersion(nifiUpgradeConfigDir,nifiUpgradeLibDir)

        if (nifiCurrentVersion == null) {
            throw new IllegalArgumentException('Could not determine current nifi version')
        }

        if (nifiUpgradeVersion == null) {
            throw new IllegalArgumentException('Could not determine upgrade nifi version')
        }

        final List<File> nifiConfigFiles = Lists.newArrayList(nifiConfDir.listFiles())
        nifiConfigFiles.add(boostrapConf)

        //obtain the rule directories sorted for each version in between current version and upgrade
        final List<String> ruleDirs = getRulesDirectoryName(nifiCurrentVersion,nifiUpgradeVersion)

        //iterate through all rule scripts in each directory and apply to file
        if(ruleDirs != null) {

            nifiConfigFiles.each { file ->

                if (!file.isDirectory()) {

                    final String scriptName = getScriptRuleName(file.getName())
                    def byte[] content = file.bytes
                    def upgradeFile = getUpgradeFile(nifiUpgradeConfigDir, file.name)

                    ruleDirs.each { ruleDir ->

                        final File script = new File(ruleDir + File.separator + scriptName)

                        if (script.exists() && supportedVersion(script, nifiCurrentVersion)) {

                            if (isVerbose) {
                                logger.info('Applying rules to {} from directory {} ', file.name, ruleDir)
                            }

                            content = migrateContent(script, content, upgradeFile.exists() ? upgradeFile.bytes : new byte[0])

                        } else {
                            if (isVerbose) {
                                logger.info('No migration rule exists in {} for file {}. ',ruleDir,file.getName())
                            }
                        }

                    }

                    //if file is external from current installation and overwrite is allowed then write to external location
                    //otherwise write to new/upgraded location
                    if (file.parentFile.parentFile!= null && file.parentFile.parentFile.toString() != nifiCurrentDir && this.overwrite) {
                        Files.write(content, file)
                    } else {
                        Files.write(content, upgradeFile)
                    }

                }else{
                    if(!this.overwrite){
                        FileUtils.copyDirectoryToDirectory(file, nifiUpgradeConfigDir)
                    }
                }
            }

        }else{
            if(isVerbose) {
                logger.info('No upgrade rules are required for these configurations.')
            }
            if(!this.overwrite){

                if(isVerbose) {
                    logger.info('Copying configurations over to upgrade directory')
                }

                nifiConfigFiles.each { file ->
                    if(file.isDirectory()){
                        FileUtils.copyDirectoryToDirectory(file, nifiUpgradeConfigDir)
                    }else {
                        FileUtils.copyFileToDirectory(file, nifiUpgradeConfigDir)
                    }
                }
            }
        }

    }

    public void run(final String nifiCurrentDir, final String bootstrapConfFile, final String nifiUpgDirString) throws ParseException, IllegalArgumentException {

        Path bootstrapConfPath = Paths.get(bootstrapConfFile)
        File bootstrapConf = Paths.get(bootstrapConfFile).toFile()

        if (!bootstrapConf.exists()) {
            throw new IllegalArgumentException('NiFi Bootstrap File provided does not exist: ' + bootstrapConfFile)
        }

        AdminUtil.with{

            Properties bootstrapProperties = getBootstrapConf(bootstrapConfPath)
            File nifiConfDir = new File(getRelativeDirectory(bootstrapProperties.getProperty('conf.dir'), nifiCurrentDir))
            File nifiLibDir = new File(getRelativeDirectory(bootstrapProperties.getProperty('lib.dir'), nifiCurrentDir))
            final File nifiUpgradeConfDir = Paths.get(nifiUpgDirString,'conf').toFile()
            final File nifiUpgradeLibDir = Paths.get(nifiUpgDirString,'lib').toFile()

            if(supportedNiFiMinimumVersion(nifiConfDir.canonicalPath, nifiLibDir.canonicalPath, SUPPORTED_MINIMUM_VERSION) &&
               supportedNiFiMinimumVersion(nifiUpgradeConfDir.canonicalPath, nifiUpgradeLibDir.canonicalPath, SUPPORTED_MINIMUM_VERSION)) {

                if (!nifiConfDir.exists() || !nifiConfDir.isDirectory()) {
                    throw new IllegalArgumentException('NiFi Configuration Directory provided is not valid: ' + nifiConfDir.absolutePath)
                }

                if (!nifiUpgradeConfDir.exists() || !nifiUpgradeConfDir.isDirectory()) {
                    throw new IllegalArgumentException('Upgrade Configuration Directory provided is not valid: ' + nifiUpgradeConfDir)
                }

                if (isVerbose) {
                    logger.info('Migrating configurations from {} to {}', nifiConfDir.absolutePath, nifiUpgradeConfDir.absolutePath)
                }

                migrate(nifiConfDir,nifiLibDir,nifiUpgradeConfDir,nifiUpgradeLibDir,bootstrapConf,nifiCurrentDir)

                if (isVerbose) {
                    logger.info('Migration completed.')
                }

            }else{
                throw new UnsupportedOperationException('Config Migration Tool only supports NiFi version 1.0.0 and above')
            }
        }
    }


}