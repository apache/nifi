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

package org.apache.nifi.toolkit.admin.filemanager

import com.google.common.collect.Sets
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.utils.IOUtils
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.toolkit.admin.AbstractAdminTool
import org.apache.nifi.toolkit.admin.configmigrator.ConfigMigrator
import org.apache.nifi.toolkit.admin.util.AdminUtil
import org.apache.nifi.util.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.PosixFilePermission

public class FileManagerTool extends AbstractAdminTool{

    private static final String DEFAULT_DESCRIPTION = 'This tool is used to perform backup, install and restore activities for a NiFi node. '
    private static final String HELP_ARG = 'help'
    private static final String VERBOSE_ARG = 'verbose'
    private static final String OPERATION = 'operation'
    private static final String NIFI_CURRENT_DIR = 'nifiCurrentDir'
    private static final String NIFI_INSTALL_DIR = 'nifiInstallDir'
    private static final String NIFI_ROLLBACK_DIR = 'nifiRollbackDir'
    private static final String BACKUP_DIR = 'backupDir'
    private static final String INSTALL_FILE = 'installFile'
    private static final String MOVE_REPOSITORIES = 'moveRepositories'
    private static final String OVERWRITE_CONFIGS = 'overwriteConfigs'
    private static final String BOOTSTRAP_CONF = 'bootstrapConf'
    private boolean moveRepositories = false
    private final static String SUPPORTED_MINIMUM_VERSION = '1.0.0'
    private static final List<PosixFilePermission> POSIX_PERMISSIONS =
            [PosixFilePermission.OTHERS_EXECUTE,
             PosixFilePermission.OTHERS_WRITE,
             PosixFilePermission.OTHERS_READ,
             PosixFilePermission.GROUP_EXECUTE,
             PosixFilePermission.GROUP_WRITE,
             PosixFilePermission.GROUP_READ,
             PosixFilePermission.OWNER_EXECUTE,
             PosixFilePermission.OWNER_WRITE,
             PosixFilePermission.OWNER_READ]


    FileManagerTool() {
        header = buildHeader(DEFAULT_DESCRIPTION)
        setup()
    }

    @Override
    protected Logger getLogger() {
        LoggerFactory.getLogger(FileManagerTool)
    }

    protected Options getOptions(){
        final Options options = new Options()
        options.addOption(Option.builder('o').longOpt(OPERATION).hasArg().desc('File operation (install | backup | restore)').build())
        options.addOption(Option.builder('b').longOpt(BACKUP_DIR).hasArg().desc('Backup NiFi Directory (used with backup or restore operation)').build())
        options.addOption(Option.builder('c').longOpt(NIFI_CURRENT_DIR).hasArg().desc('Current NiFi Installation Directory (used optionally with install or restore operation)').build())
        options.addOption(Option.builder('d').longOpt(NIFI_INSTALL_DIR).hasArg().desc('NiFi Installation Directory (used with install or restore operation)').build())
        options.addOption(Option.builder('i').longOpt(INSTALL_FILE).hasArg().desc('NiFi Install File').build())
        options.addOption(Option.builder('r').longOpt(NIFI_ROLLBACK_DIR).hasArg().desc('NiFi Installation Directory (used with install or restore operation)').build())
        options.addOption(Option.builder('t').longOpt(BOOTSTRAP_CONF).hasArg().desc('Current NiFi Bootstrap Configuration File (optional)').build())
        options.addOption(Option.builder('m').longOpt(MOVE_REPOSITORIES).desc('Allow repositories to be moved to new/restored nifi directory from existing installation, if available (used optionally with install or restore operation)').build())
        options.addOption(Option.builder('x').longOpt(OVERWRITE_CONFIGS).desc('Overwrite existing configuration directory with upgrade changes (used optionally with install or restore operation)').build())
        options.addOption(Option.builder('h').longOpt(HELP_ARG).desc('Print help info (optional)').build())
        options.addOption(Option.builder('v').longOpt(VERBOSE_ARG).desc('Set mode to verbose (optional, default is false)').build())

        options
    }

    Set<PosixFilePermission> fromMode(final long mode) {

        Set<PosixFilePermission> permissions = Sets.newHashSet()

        POSIX_PERMISSIONS.eachWithIndex{
            perm,index ->
                if ((mode & (1 << index)) != 0) {
                    permissions.add(perm)
                }
        }

        permissions
    }

    Properties getProperties(Path confFileName){
        final Properties properties = new Properties()
        final File confFile = confFileName.toFile()
        properties.load(new FileInputStream(confFile))
        properties
    }

    boolean valid(File nifiDir){
        if(nifiDir.isDirectory() && Files.exists(Paths.get(nifiDir.absolutePath,'bin','nifi.sh'))){
            true
        }else {
            false
        }
    }

    void move(final String srcDir, final String oldDir, final String newDir){

        final String oldPathName = srcDir.startsWith('./') ? oldDir + File.separator + srcDir[2..-1] : oldDir + File.separator + srcDir
        final String newPathName = srcDir.startsWith('./') ? newDir + File.separator + srcDir[2..-1] : newDir + File.separator + srcDir

        final Path oldPath = Paths.get(oldPathName)
        final Path newPath = Paths.get(newPathName)

        if(Files.exists(oldPath)) {
            Files.move(oldPath, newPath)
        }

    }

    void moveRepository(final String dirName, final String installDirName){

        if(isVerbose){
            logger.info('Moving repositories from {} to {}. Please note that repositories may be upgraded during install and become incompatible with a previous version. ',dirName,installDirName)
        }

        final String bootstrapConfFileName = dirName + File.separator + 'conf' + File.separator + 'bootstrap.conf'
        final Properties bootstrapProperties = getProperties(Paths.get(bootstrapConfFileName))
        final String nifiPropertiesFile = AdminUtil.getRelativeDirectory(bootstrapProperties.getProperty('conf.dir'),dirName) + File.separator +'nifi.properties'
        final Properties nifiProperties = getProperties(Paths.get(nifiPropertiesFile))
        final String flowFileDirectory = nifiProperties.getProperty('nifi.flowfile.repository.directory')
        final String contentRepositoryDir = nifiProperties.getProperty('nifi.content.repository.directory.default')
        final String provenanceRepositoryDir = nifiProperties.getProperty('nifi.provenance.repository.directory.default')
        final String databaseDirectory = nifiProperties.getProperty('nifi.database.directory')

        if(flowFileDirectory.startsWith('./')){
            if(isVerbose){
                logger.info('Moving flowfile repo')
            }
            move(flowFileDirectory,dirName,installDirName)
        }

        if(contentRepositoryDir.startsWith('./')){
            if(isVerbose){
                logger.info('Moving content repo')
            }
            move(contentRepositoryDir,dirName,installDirName)
        }

        if(provenanceRepositoryDir.startsWith('./')){
            if(isVerbose){
                logger.info('Moving provenance repo')
            }
            move(provenanceRepositoryDir,dirName,installDirName)
        }

        if(databaseDirectory.startsWith('./')){
            if(isVerbose){
                logger.info('Moving database repo')
            }
            move(databaseDirectory,dirName,installDirName)
        }
    }

    void copyState(final String currentNiFiDirName, final String installDirName){

        File stateDir = Paths.get(currentNiFiDirName,'state').toFile()

        if(stateDir.exists()){

            if(Files.exists(Paths.get(installDirName,'state'))){
               Files.delete(Paths.get(installDirName,'state'))
            }

            FileUtils.copyDirectoryToDirectory(stateDir, Paths.get(installDirName).toFile())
        }

    }

    protected void setPosixPermissions(final ArchiveEntry entry, final File outputFile, final ZipFile zipFile){
        int mode = 0

        if (entry instanceof TarArchiveEntry) {
            mode = ((TarArchiveEntry) entry).getMode()

        }else if(entry instanceof ZipArchiveEntry && zipFile != null){
            mode = zipFile.getEntry(entry.name).getUnixMode()
        }

        if(mode == 0){
            mode = outputFile.isDirectory()? TarArchiveEntry.DEFAULT_DIR_MODE: TarArchiveEntry.DEFAULT_FILE_MODE
        }

        Set<PosixFilePermission> permissions = fromMode(mode)
        if(permissions.size() > 0) {
            Files.setPosixFilePermissions(outputFile.toPath(), fromMode(mode))
        }

    }

    protected void setPosixPermissions(final File file,List<PosixFilePermission> permissions = []){

        if (SystemUtils.IS_OS_WINDOWS) {
            file?.setReadable(permissions.contains(PosixFilePermission.OWNER_READ))
            file?.setWritable(permissions.contains(PosixFilePermission.OWNER_WRITE))
            file?.setExecutable(permissions.contains(PosixFilePermission.OWNER_EXECUTE))
        } else {
            Files.setPosixFilePermissions(file?.toPath(), permissions as Set)
        }

    }

    void backup(String backupNiFiDirName, String currentNiFiDirName, String bootstrapConfFileName){

        if(isVerbose){
            logger.info('Creating backup in directory {}. Please note that repositories are not included in backup operation.',backupNiFiDirName)
        }

        final File backupNiFiDir = new File(backupNiFiDirName)
        final Properties bootstrapProperties =  getProperties(Paths.get(bootstrapConfFileName))
        final File confDir = new File(AdminUtil.getRelativeDirectory(bootstrapProperties.getProperty('conf.dir'),currentNiFiDirName))
        final File libDir  = new File(AdminUtil.getRelativeDirectory(bootstrapProperties.getProperty('lib.dir'),currentNiFiDirName))

        if( backupNiFiDir.exists() && backupNiFiDir.isDirectory()){
            backupNiFiDir.deleteDir()
        }

        backupNiFiDir.mkdirs()

        Files.createDirectory(Paths.get(backupNiFiDirName,'bootstrap_files'))
        FileUtils.copyFileToDirectory(Paths.get(bootstrapConfFileName).toFile(),Paths.get(backupNiFiDirName,'bootstrap_files').toFile())
        FileUtils.copyDirectoryToDirectory(Paths.get(currentNiFiDirName,'lib','bootstrap').toFile(),Paths.get(backupNiFiDirName,'bootstrap_files').toFile())
        Files.createDirectories(Paths.get(backupNiFiDirName,'conf'))
        Files.createDirectories(Paths.get(backupNiFiDirName,'lib'))
        FileUtils.copyDirectoryToDirectory(confDir,Paths.get(backupNiFiDirName).toFile())
        FileUtils.copyDirectoryToDirectory(libDir,Paths.get(backupNiFiDirName).toFile())
        FileUtils.copyDirectoryToDirectory(Paths.get(currentNiFiDirName,'bin').toFile(),new File(backupNiFiDirName))
        FileUtils.copyDirectoryToDirectory(Paths.get(currentNiFiDirName,'docs').toFile(),new File(backupNiFiDirName))
        FileUtils.copyFileToDirectory(Paths.get(currentNiFiDirName,'LICENSE').toFile(),new File(backupNiFiDirName))
        FileUtils.copyFileToDirectory(Paths.get(currentNiFiDirName,'NOTICE').toFile(),new File(backupNiFiDirName))
        FileUtils.copyFileToDirectory(Paths.get(currentNiFiDirName,'README').toFile(),new File(backupNiFiDirName))

        if(isVerbose){
            logger.info('Backup Complete')
        }

    }

    void restore(String backupNiFiDirName, String rollbackNiFiDirName, String currentNiFiDirName, String bootstrapConfFileName){

        if(isVerbose){
            logger.info('Restoring to directory:' + rollbackNiFiDirName)
        }

        final File rollbackNiFiDir = new File(rollbackNiFiDirName)
        final File rollbackNiFiLibDir = Paths.get(rollbackNiFiDirName,'lib').toFile()
        final File rollbackNiFiConfDir = Paths.get(rollbackNiFiDirName,'conf').toFile()
        final Properties bootstrapProperties =  getProperties(Paths.get(backupNiFiDirName,'bootstrap_files','bootstrap.conf'))
        final File confDir = new File(AdminUtil.getRelativeDirectory(bootstrapProperties.getProperty('conf.dir'),rollbackNiFiDirName))
        final File libDir  = new File(AdminUtil.getRelativeDirectory(bootstrapProperties.getProperty('lib.dir'),rollbackNiFiDirName))


        if(!rollbackNiFiDir.isDirectory()){
            rollbackNiFiDir.mkdirs()
        }

        if(!rollbackNiFiLibDir.isDirectory()){
            rollbackNiFiLibDir.mkdirs()
        }

        if(!rollbackNiFiConfDir.isDirectory()){
            rollbackNiFiConfDir.mkdirs()
        }

        if(!libDir.isDirectory()){
            libDir.mkdirs()
        }

        if(!confDir.isDirectory()){
            confDir.mkdirs()
        }

        FileUtils.copyFile(Paths.get(backupNiFiDirName,'bootstrap_files','bootstrap.conf').toFile(), new File(bootstrapConfFileName))
        FileUtils.copyDirectoryToDirectory(Paths.get(backupNiFiDirName,'bootstrap_files','bootstrap').toFile(),Paths.get(rollbackNiFiDirName,'lib').toFile())
        FileUtils.copyDirectoryToDirectory(Paths.get(backupNiFiDirName,'bin').toFile(),new File(rollbackNiFiDirName))
        FileUtils.copyDirectoryToDirectory(Paths.get(backupNiFiDirName,'docs').toFile(),new File(rollbackNiFiDirName))
        FileUtils.copyDirectory(Paths.get(backupNiFiDirName,'lib').toFile(),libDir)
        FileUtils.copyDirectory(Paths.get(backupNiFiDirName,'conf').toFile(),confDir)
        FileUtils.copyFileToDirectory(Paths.get(backupNiFiDirName,'LICENSE').toFile(),new File(rollbackNiFiDirName))
        FileUtils.copyFileToDirectory(Paths.get(backupNiFiDirName,'NOTICE').toFile(),new File(rollbackNiFiDirName))
        FileUtils.copyFileToDirectory(Paths.get(backupNiFiDirName,'README').toFile(),new File(rollbackNiFiDirName))

        final File binDir = Paths.get(rollbackNiFiDirName,'bin').toFile()
        binDir.listFiles().each { setPosixPermissions(it,[PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
                                                     PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ,
                                                     PosixFilePermission.OTHERS_EXECUTE]) }

        if(currentNiFiDirName && moveRepositories) {
            moveRepository(currentNiFiDirName, rollbackNiFiDirName)
        }

        if(isVerbose){
            logger.info('Restore Completed.')
        }

    }

    String extract(final File installFile, final File installDirName){

        if(isVerbose){
            logger.info('Beginning extraction using {} into installation directory {}',installFile.absolutePath,installDirName.absolutePath)
        }

        final String extension = FilenameUtils.getExtension(installFile.getName())
        final InputStream fis = extension.equals('gz') ? new GzipCompressorInputStream(new FileInputStream(installFile)) : new FileInputStream(installFile)
        final ArchiveInputStream inputStream = new ArchiveStreamFactory().createArchiveInputStream(new BufferedInputStream(fis))
        final ZipFile zipFile = extension.equals('zip') ? new ZipFile(installFile) : null

        ArchiveEntry entry = inputStream.nextEntry

        if(entry != null){

            String archiveRootDir = null

            while(entry != null){

                if(archiveRootDir == null & entry.name.toLowerCase().startsWith('nifi')){

                    archiveRootDir = entry.name.indexOf(File.separator) > -1 ? entry.name[0..entry.name.indexOf(File.separator)-1]  : entry.name

                    if(isVerbose){
                        logger.info('Upgrade root directory: {}', archiveRootDir)
                    }

                    File archiveRootDirFile = Paths.get(installDirName.getAbsolutePath(),archiveRootDir).toFile()

                    if(archiveRootDirFile.exists()){
                        archiveRootDirFile.deleteDir()
                    }
                    archiveRootDirFile.mkdirs()
                }

                if(isVerbose){
                    logger.info('Extracting file: {} ',entry.name)
                }

                if(archiveRootDir && entry.name.startsWith(archiveRootDir)) {

                    final File outputFile = Paths.get(installDirName.getAbsolutePath(),entry.name).toFile()

                    if (entry.isDirectory()) {

                        if (!outputFile.exists()) {
                            if (!outputFile.mkdirs()) {
                                throw new IllegalStateException('Could not create directory :' + outputFile.getAbsolutePath())
                            }
                        }

                    } else {

                        File parentDirectory = outputFile.getParentFile()

                        if(!parentDirectory.exists()){
                            parentDirectory.mkdirs()
                        }

                        final OutputStream outputFileStream = new FileOutputStream(outputFile)
                        IOUtils.copy(inputStream, outputFileStream)
                        outputFileStream.close()
                    }

                    if(!SystemUtils.IS_OS_WINDOWS){
                        setPosixPermissions(entry,outputFile,zipFile)
                    }
                }

                entry = inputStream.nextEntry
            }

            return archiveRootDir

        }else{
            throw new RuntimeException('Attempting to extract installation file however it is empty: '+installFile.getName())
        }

    }

    void install(final String installFileName, final String installDirName, final String currentNiFiDirName, final String bootstrapConfFileName, final Boolean overwriteConfigs){

        final File installFile = new File(installFileName)

        if(isVerbose){
            logger.info('Beginning installation into directory:' + installDirName)
        }

        if(installFile.exists()){

            final File installDir = new File(installDirName)

            if(!installDir.exists()){
                installDir.mkdirs()
            }

            final String installRootDirName = extract(installFile,installDir)
            final File installRootDir = Paths.get(installDirName,installRootDirName).toFile()

            if(valid(installRootDir)){

                if(currentNiFiDirName && bootstrapConfFileName){
                    copyState(currentNiFiDirName,installRootDir.absolutePath)
                    if(moveRepositories) {
                        moveRepository(currentNiFiDirName,installRootDir.absolutePath)
                    }
                    final ConfigMigrator configMigrator = new ConfigMigrator(isVerbose,overwriteConfigs)
                    configMigrator.run(currentNiFiDirName,bootstrapConfFileName,installRootDir.canonicalPath)
                }

            }else{
                throw new RuntimeException('Extract failed: Invalid NiFi Installation. Check the install path provided and retry.')
            }

        }else{
            throw new RuntimeException('Installation file provided does not exist')
        }

        if(isVerbose){
            logger.info('Installation Complete')
        }

    }

    void parseInstall(final CommandLine commandLine){

        if(commandLine.hasOption(MOVE_REPOSITORIES)){
            this.moveRepositories = true
        }

        if(!commandLine.hasOption(INSTALL_FILE)){
            throw new ParseException('Missing -i option')
        } else if(!commandLine.hasOption(NIFI_INSTALL_DIR)){
            throw new ParseException('Missing -d option')
        } else if (!commandLine.hasOption(NIFI_CURRENT_DIR) && moveRepositories){
            throw new ParseException('Missing -c option: Moving repositories requires current nifi directory')
        }

        final String installFileName = commandLine.getOptionValue(INSTALL_FILE)
        final String nifiCurrentDirName = commandLine.getOptionValue(NIFI_CURRENT_DIR)
        final String nifiInstallDirName = commandLine.getOptionValue(NIFI_INSTALL_DIR)
        final Boolean overwriteConfigs = commandLine.hasOption(OVERWRITE_CONFIGS)
        final String bootstrapConfFileName = commandLine.hasOption(BOOTSTRAP_CONF) ? commandLine.getOptionValue(BOOTSTRAP_CONF) : nifiCurrentDirName ?
                                                                                     Paths.get(nifiCurrentDirName,'conf','bootstrap.conf').toString() : null

        if (Files.notExists(Paths.get(installFileName))) {
            throw new ParseException('Missing installation file: ' + installFileName)
        }

        if (nifiCurrentDirName && Files.notExists(Paths.get(nifiCurrentDirName))) {
            throw new ParseException('Current NiFi installation path does not exist: ' + nifiCurrentDirName)
        }

        if(nifiCurrentDirName && bootstrapConfFileName && !supportedNiFiMinimumVersion(nifiCurrentDirName, bootstrapConfFileName, SUPPORTED_MINIMUM_VERSION)) {
            throw new UnsupportedOperationException('File Manager Tool only supports NiFi versions 1.0.0 or higher.')
        }

        install(installFileName, nifiInstallDirName, !nifiCurrentDirName ? null : Paths.get(nifiCurrentDirName).toFile().getCanonicalPath(), bootstrapConfFileName, overwriteConfigs)

    }

    void parseBackup(final CommandLine commandLine){

        if(!commandLine.hasOption(BACKUP_DIR)){
            throw new ParseException('Missing -b option')
        } else if(!commandLine.hasOption(NIFI_CURRENT_DIR)){
            throw new ParseException('Missing -c option')
        }

        final String backupDirName = commandLine.getOptionValue(BACKUP_DIR)
        final String nifiCurrentDirName = commandLine.getOptionValue(NIFI_CURRENT_DIR)

        if (Files.notExists(Paths.get(nifiCurrentDirName))) {
            throw new ParseException('Current NiFi installation link does not exist: ' + nifiCurrentDirName)
        }

        final String bootstrapConfFileName = commandLine.hasOption(BOOTSTRAP_CONF) ? commandLine.getOptionValue(BOOTSTRAP_CONF) : Paths.get(nifiCurrentDirName,'conf','bootstrap.conf').toString()

        if(supportedNiFiMinimumVersion(nifiCurrentDirName, bootstrapConfFileName, SUPPORTED_MINIMUM_VERSION)) {
            backup(backupDirName, Paths.get(nifiCurrentDirName).toFile().getCanonicalPath(),bootstrapConfFileName)
        }else{
            throw new UnsupportedOperationException('File Manager Tool only supports NiFi versions 1.0.0 or higher.')
        }

    }

    void parseRestore(final CommandLine commandLine){

        if(commandLine.hasOption(MOVE_REPOSITORIES)){
            this.moveRepositories = true
        }

        if(!commandLine.hasOption(BACKUP_DIR)) {
            throw new ParseException('Missing -b option')
        }else if(!commandLine.hasOption(NIFI_ROLLBACK_DIR)){
            throw new ParseException('Missing -r option')
        }else if (!commandLine.hasOption(NIFI_CURRENT_DIR) && moveRepositories){
            throw new ParseException('Missing -c option: Moving repositories requires current nifi directory')
        }

        final String backupDirName = commandLine.getOptionValue(BACKUP_DIR)
        final String nifiRollbackDirName = commandLine.getOptionValue(NIFI_ROLLBACK_DIR)
        final String nifiCurrentDirName = commandLine.getOptionValue(NIFI_CURRENT_DIR)

        if (Files.notExists(Paths.get(backupDirName)) || !Files.isDirectory(Paths.get(backupDirName))) {
            throw new ParseException('Missing or invalid backup directory: ' + backupDirName)
        }

        if (nifiCurrentDirName && Files.notExists(Paths.get(nifiCurrentDirName))) {
            throw new ParseException('Current NiFi installation path does not exist: ' + nifiCurrentDirName)
        }

        if(!supportedNiFiMinimumVersion(backupDirName, Paths.get(backupDirName,'bootstrap_files','bootstrap.conf').toString(), SUPPORTED_MINIMUM_VERSION)) {
            throw new UnsupportedOperationException('File Manager Tool only supports NiFi versions 1.0.0 or higher.')
        }

        final String bootstrapConfFileName = commandLine.hasOption(BOOTSTRAP_CONF) ? commandLine.getOptionValue(BOOTSTRAP_CONF) : Paths.get(nifiRollbackDirName,'conf','bootstrap.conf').toString()
        restore(backupDirName, nifiRollbackDirName, !nifiCurrentDirName ? null : Paths.get(nifiCurrentDirName).toFile().getCanonicalPath(), bootstrapConfFileName)

    }

    void parse(final String[] args) throws ParseException, IllegalArgumentException {

        CommandLine commandLine = new DefaultParser().parse(options, args)

        if (commandLine.hasOption(HELP_ARG)) {
            printUsage(null)
        } else if (commandLine.hasOption(OPERATION)) {

            if(commandLine.hasOption(VERBOSE_ARG)){
                this.isVerbose = true
            }

            String operation = commandLine.getOptionValue(OPERATION).toLowerCase()

            if(operation.equals('install')){
                parseInstall(commandLine)
            }else if(operation.equals('backup')){
                parseBackup(commandLine)
            }else if(operation.equals('restore')){
                parseRestore(commandLine)
            }else{
                throw new ParseException('Invalid operation value:' + operation)
            }

        }else{
            throw new ParseException('Missing -o option')
        }

    }

    public static void main(String[] args) {
        FileManagerTool tool = new FileManagerTool()

        try {
            tool.parse(args)
        } catch (Exception e) {
            tool.printUsage(e.getLocalizedMessage())
            System.exit(1)
        }

        System.exit(0)
    }


}
