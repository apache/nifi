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

import org.apache.commons.cli.ParseException
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.SystemUtils
import org.junit.Rule
import org.junit.contrib.java.lang.system.ExpectedSystemExit
import org.junit.contrib.java.lang.system.SystemOutRule
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission

class FileManagerToolSpec extends Specification{
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none()

    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog()


    def "print help and usage info"() {

        given:
        def manager = new FileManagerTool()

        when:
        manager.parse(["-h"] as String[])

        then:
        systemOutRule.getLog().contains("usage: org.apache.nifi.toolkit.admin.filemanager.FileManagerTool")
    }

    def "throws exception missing operation flag"() {

        given:
        def manager = new FileManagerTool()

        when:
        manager.parse(["-d", "/missing/upgrade/dir"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -o option"
    }

    def "throws exception if missing upgrade file for install"() {

        given:
        def manager = new FileManagerTool()

        when:
        manager.parse(["-o", "install","-d","/missing/upgrade/dir"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -i option"
    }

    def "throws exception if missing install directory for install"() {

        given:
        def manager = new FileManagerTool()

        when:
        manager.parse(["-o", "install","-i","/missing/upgrade/dir"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -d option"
    }

    def "throws exception if missing current directory when moving repositories for install"() {

        given:
        def manager = new FileManagerTool()

        when:
        manager.parse(["-o", "install","-i","/missing/current/dir","-d","/missing/current/dir","-m"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -c option: Moving repositories requires current nifi directory"
    }


    def "throws exception if missing backup directory for backup"() {

        given:
        def manager = new FileManagerTool()

        when:
        manager.parse(["-o", "backup","-c","/missing/backup/dir"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -b option"
    }

    def "throws exception if missing current dir for backup"() {

        given:
        def manager = new FileManagerTool()

        when:
        manager.parse(["-o", "backup","-b","/missing/current/dir"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -c option"
    }

    def "throws exception if missing rollback directory for restore"() {

        given:
        def manager = new FileManagerTool()

        when:
        manager.parse(["-o", "restore","-b","/missing/rollback/dir"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -r option"
    }

    def "throws exception if missing backup directory for restore"() {

        given:
        def manager = new FileManagerTool()

        when:
        manager.parse(["-o", "restore","-r","/missing/backup/dir","-c","/missing/rollback/dir"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -b option"
    }

    def "throws exception if missing current directory when wanting to move repositories during install"() {

        given:
        def manager = new FileManagerTool()

        when:
        manager.parse(["-o", "restore","-r","/missing/current/dir","-b","/missing/current/dir","-m"] as String[])

        then:
        def e = thrown(ParseException)
        e.message == "Missing -c option: Moving repositories requires current nifi directory"
    }


    def "move directory from src to target"(){

        setup:
        def File tmpDir = setupTmpDir()
        def File testDir = new File("target/tmp/conf/test")
        def File oldConfDir= new File("target/tmp/conf")
        def File newConfDir= new File("target/tmp/new_conf")
        oldConfDir.mkdirs()
        newConfDir.mkdirs()
        testDir.mkdirs()
        def manager = new FileManagerTool()
        def File newTestDir = new File("target/tmp/new_conf/test")

        when:
        manager.move("./test","target/tmp/conf","target/tmp/new_conf")

        then:
        newTestDir.exists()

        cleanup:
        tmpDir.deleteDir()

    }

    def "move zookeeper from src to target"(){

        setup:
        def File tmpDir = setupTmpDir()
        def manager = new FileManagerTool()
        def File oldNiFiDir = new File("target/tmp/nifi")
        def File zookeeperDir = new File("target/tmp/nifi/state/zookeeper")
        def File upgradeNiFiDir= new File("target/tmp/nifi_upgrade")
        def myid = new File("src/test/resources/filemanager/myid")
        oldNiFiDir.mkdirs()
        zookeeperDir.mkdirs()
        upgradeNiFiDir.mkdirs()

        FileUtils.copyFileToDirectory(myid,zookeeperDir)

        when:
        manager.copyState("target/tmp/nifi","target/tmp/nifi_upgrade")
        def File upgradeMyid = new File("target/tmp/nifi_upgrade/state/zookeeper/myid")

        then:
        upgradeMyid.exists()

        cleanup:
        tmpDir.deleteDir()

    }

    def "move repositories from src to target"(){

        setup:
        def File tmpDir = setupTmpDir()
        def manager = new FileManagerTool()

        def File oldNiFiDir = new File("target/tmp/nifi")
        def File oldNiFiConfDir = new File("target/tmp/nifi/conf")
        def File upgradeNiFiDir= new File("target/tmp/nifi_upgrade")

        oldNiFiDir.mkdirs()
        oldNiFiConfDir.mkdirs()
        upgradeNiFiDir.mkdirs()

        def bootstrapFile = new File("src/test/resources/filemanager/bootstrap.conf")
        def nifiProperties = new File("src/test/resources/filemanager/nifi.properties")
        def File flowfileRepositoryDir = new File("target/tmp/nifi/flowfile_repository")
        def File contentRepositoryDir = new File("target/tmp/nifi/content_repository")
        def File databaseRepositoryDir = new File("target/tmp/nifi/database_repository")
        def File provenanceRepositoryDir = new File("target/tmp/nifi/provenance_repository")

        FileUtils.copyFileToDirectory(bootstrapFile,oldNiFiConfDir)
        FileUtils.copyFileToDirectory(nifiProperties,oldNiFiConfDir)
        flowfileRepositoryDir.mkdirs()
        contentRepositoryDir.mkdirs()
        databaseRepositoryDir.mkdirs()
        provenanceRepositoryDir.mkdirs()

        when:
        manager.moveRepository("target/tmp/nifi","target/tmp/nifi_upgrade")
        def files = upgradeNiFiDir.listFiles()
        def count = files.findAll { it.name in ["flowfile_repository","content_repository","database_repository","provenance_repository"]}.size()

        then:
        count == 4

        cleanup:
        tmpDir.deleteDir()

    }

    def "backup nifi installation successfully"(){

        setup:
        def File tmpDir = setupTmpDir()
        def manager = new FileManagerTool()
        def File niFiDir = new File("target/tmp/nifi")
        def File niFiConfDir = new File("target/tmp/nifi/conf")
        def File backupNiFiDir= new File("target/tmp/nifi_bak")
        def File binDir = new File("target/tmp/nifi/bin")
        def File nifiShell = new File("target/tmp/nifi/bin/nifi.sh")
        def File libDir = new File("target/tmp/nifi/lib")
        def File bootstrapLibDir = new File("target/tmp/nifi/lib/bootstrap")
        def File docDir = new File("target/tmp/nifi/docs")
        def bootstrapFile = new File("src/test/resources/filemanager/bootstrap.conf")
        def nifiProperties = new File("src/test/resources/filemanager/nifi.properties")
        def license = new File("target/tmp/nifi/LICENSE")
        def notice = new File("target/tmp/nifi/NOTICE")
        def readme = new File("target/tmp/nifi/README")

        binDir.mkdirs()
        libDir.mkdirs()
        bootstrapLibDir.mkdirs()
        docDir.mkdirs()
        niFiDir.mkdirs()
        niFiConfDir.mkdirs()
        nifiShell.write("shell")
        license.write("license")
        readme.write("readme")
        notice.write("notice")
        FileUtils.copyFileToDirectory(bootstrapFile,niFiConfDir)
        FileUtils.copyFileToDirectory(nifiProperties,niFiConfDir)

        when:
        manager.backup("target/tmp/nifi_bak","target/tmp/nifi","target/tmp/nifi/conf/bootstrap.conf")

        then:
        backupNiFiDir.exists()
        def files = backupNiFiDir.listFiles()
        def expectedFiles = ["bin","lib","docs","README","LICENSE","NOTICE","conf","bootstrap_files"]
        def count = files.findAll {it.name in expectedFiles}.size()
        count == expectedFiles.size()

        cleanup:
        tmpDir.deleteDir()

    }

    def "restore nifi installation successfully"(){

        setup:
        def File tmpDir = setupTmpDir()
        def manager = new FileManagerTool()
        def File rollbackNiFiDir= new File("target/tmp/nifi_1")
        def File rollbackNiFiLibDir = new File("target/tmp/nifi_1/lib")
        def File currentNiFiConfDir= new File("target/tmp/nifi/conf")
        def File backupNiFiDir = new File("target/tmp/nifi_bak")
        def File backupNiFiConfDir = new File("target/tmp/nifi_bak/conf")
        def File backupBinDir = new File("target/tmp/nifi_bak/bin")
        def File backupNiFiShell = new File("target/tmp/nifi_bak/bin/nifi.sh")
        def File backupLibDir = new File("target/tmp/nifi_bak/lib")
        def File bootstrapDir = new File("target/tmp/nifi_bak/bootstrap_files")
        def File bootstrapLibDir = new File("target/tmp/nifi_bak/bootstrap_files/bootstrap")
        def File backupDocDir = new File("target/tmp/nifi_bak/docs")

        def bootstrapFile = new File("src/test/resources/filemanager/bootstrap.conf")
        def nifiProperties = new File("src/test/resources/filemanager/nifi.properties")
        def license = new File("target/tmp/nifi_bak/LICENSE")
        def notice = new File("target/tmp/nifi_bak/NOTICE")
        def readme = new File("target/tmp/nifi_bak/README")
        def libjar = new File("target/tmp/nifi_bak/lib/lib.jar")
        def File flowfileRepositoryDir = new File("target/tmp/nifi/flowfile_repository")
        def File contentRepositoryDir = new File("target/tmp/nifi/content_repository")
        def File databaseRepositoryDir = new File("target/tmp/nifi/database_repository")
        def File provenanceRepositoryDir = new File("target/tmp/nifi/provenance_repository")

        currentNiFiConfDir.mkdirs()
        backupNiFiDir.mkdirs()
        backupNiFiConfDir.mkdirs()
        bootstrapDir.mkdirs()
        bootstrapLibDir.mkdirs()
        backupDocDir.mkdirs()
        backupNiFiDir.mkdirs()
        backupNiFiConfDir.mkdirs()
        backupBinDir.mkdirs()
        backupLibDir.mkdirs()
        backupNiFiShell.write("shell")
        license.write("license")
        readme.write("readme")
        notice.write("notice")
        libjar.write("fakejar")
        flowfileRepositoryDir.mkdirs()
        contentRepositoryDir.mkdirs()
        databaseRepositoryDir.mkdirs()
        provenanceRepositoryDir.mkdirs()

        FileUtils.copyFileToDirectory(bootstrapFile,bootstrapDir)
        FileUtils.copyFileToDirectory(bootstrapFile,currentNiFiConfDir)
        FileUtils.copyFileToDirectory(nifiProperties,backupNiFiConfDir)
        FileUtils.copyFileToDirectory(nifiProperties,currentNiFiConfDir)

        when:
        manager.restore("target/tmp/nifi_bak","target/tmp/nifi_1","target/tmp/nifi","target/tmp/nifi_1/conf/boostrap.conf")

        then:
        rollbackNiFiDir.exists()
        rollbackNiFiLibDir.exists()
        def files = rollbackNiFiDir.listFiles()
        def expectedFiles = ["bin","lib","docs","README","LICENSE","NOTICE","conf"]
        def count = files.findAll {it.name in expectedFiles}.size()
        count == expectedFiles.size()
        def libFiles = rollbackNiFiLibDir.listFiles()
        libFiles.findAll{it.name == "lib.jar"}.size() == 1

        cleanup:
        tmpDir.deleteDir()

    }

    def "extract compressed tar file successfully"(){

        setup:
        def File tmpDir = setupTmpDir()
        def manager = new FileManagerTool()
        def File nifiArchive = new File("src/test/resources/filemanager/nifi-test-archive.tar.gz")
        def File nifiInstallDir = new File("target/tmp/nifi_tar")
        def File nifiInstallBinDir = new File("target/tmp/nifi_tar/nifi-test-archive/bin")

        nifiInstallDir.mkdirs()

        when:
        manager.extract(nifiArchive,nifiInstallDir)

        then:
        nifiInstallBinDir.exists()

        cleanup:
        tmpDir.deleteDir()

    }

    def "extract zip file successfully"(){

        setup:
        def File tmpDir = setupTmpDir()
        def manager = new FileManagerTool()
        def File nifiArchive = new File("src/test/resources/filemanager/nifi-test-archive.zip")
        def File nifiInstallDir = new File("target/tmp/nifi_zip")
        def File nifiInstallBinDir = new File("target/tmp/nifi_zip/nifi-test-archive/bin")

        nifiInstallDir.mkdirs()

        when:
        def upgradeRoot = manager.extract(nifiArchive,nifiInstallDir)

        then:
        upgradeRoot == "nifi-test-archive"
        nifiInstallBinDir.exists()

        cleanup:
        tmpDir.deleteDir()

    }

    def "install nifi with existing installation successfully"(){
        setup:
        def File tmpDir = setupTmpDir()
        def manager = new FileManagerTool()
        def File nifiArchive = new File("src/test/resources/filemanager/nifi-test-archive.tar.gz")
        def bootstrapFile = new File("src/test/resources/filemanager/bootstrap.conf")
        def nifiProperties = new File("src/test/resources/filemanager/nifi.properties")
        def File nifiCurrentDir = new File("target/tmp/nifi_old")
        def File nifiCurrentConfDir = new File("target/tmp/nifi_old/conf")
        def File nifiInstallDir = new File("target/tmp/nifi")
        def File nifiInstallBinDir = new File("target/tmp/nifi/nifi-test-archive/bin")
        def File flowfileRepositoryDir = new File("target/tmp/nifi_old/flowfile_repository")
        def File contentRepositoryDir = new File("target/tmp/nifi_old/content_repository")
        def File databaseRepositoryDir = new File("target/tmp/nifi_old/database_repository")
        def File provenanceRepositoryDir = new File("target/tmp/nifi_old/provenance_repository")

        nifiInstallDir.mkdirs()
        nifiCurrentDir.mkdirs()
        flowfileRepositoryDir.mkdirs()
        contentRepositoryDir.mkdirs()
        databaseRepositoryDir.mkdirs()
        provenanceRepositoryDir.mkdirs()
        FileUtils.copyFileToDirectory(bootstrapFile,nifiCurrentConfDir)
        FileUtils.copyFileToDirectory(nifiProperties,nifiCurrentConfDir)

        when:
        manager.install(nifiArchive.getAbsolutePath(),nifiInstallDir.getAbsolutePath(),nifiCurrentDir.getAbsolutePath(),"target/tmp/nifi_old/conf/bootstrap.conf",false)

        then:
        nifiInstallBinDir.exists()

        cleanup:
        tmpDir.deleteDir()

    }

    def "install nifi without existing installation successfully"(){
        setup:
        def File tmpDir = setupTmpDir()
        def manager = new FileManagerTool()
        def File nifiArchive = new File("src/test/resources/filemanager/nifi-test-archive.tar.gz")
        def bootstrapFile = new File("src/test/resources/filemanager/bootstrap.conf")
        def nifiProperties = new File("src/test/resources/filemanager/nifi.properties")
        def File nifiInstallDir = new File("target/tmp/nifi")
        def File nifiInstallBinDir = new File("target/tmp/nifi/nifi-test-archive/bin")
        def File flowfileRepositoryDir = new File("target/tmp/nifi_old/flowfile_repository")
        def File contentRepositoryDir = new File("target/tmp/nifi_old/content_repository")
        def File databaseRepositoryDir = new File("target/tmp/nifi_old/database_repository")
        def File provenanceRepositoryDir = new File("target/tmp/nifi_old/provenance_repository")

        nifiInstallDir.mkdirs()

        flowfileRepositoryDir.mkdirs()
        contentRepositoryDir.mkdirs()
        databaseRepositoryDir.mkdirs()
        provenanceRepositoryDir.mkdirs()

        when:
        manager.install(nifiArchive.getAbsolutePath(),nifiInstallDir.getAbsolutePath(),null,null,false)

        then:
        nifiInstallBinDir.exists()

        cleanup:
        tmpDir.deleteDir()

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
