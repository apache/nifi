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

package org.apache.nifi.toolkit.admin.notify

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.apache.commons.lang3.StringUtils
import org.apache.nifi.properties.NiFiPropertiesLoader
import org.apache.nifi.toolkit.admin.AbstractAdminTool
import org.apache.nifi.toolkit.admin.client.ClientFactory
import org.apache.nifi.toolkit.admin.client.NiFiClientFactory
import org.apache.nifi.toolkit.admin.client.NiFiClientUtil
import org.apache.nifi.toolkit.admin.util.AdminUtil
import org.apache.nifi.util.NiFiBootstrapUtils
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.web.api.dto.BulletinDTO
import org.apache.nifi.web.api.entity.BulletinEntity
import org.apache.nifi.web.security.ProxiedEntitiesUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.ws.rs.client.Client
import javax.ws.rs.client.Entity
import javax.ws.rs.client.WebTarget
import javax.ws.rs.core.Response
import java.nio.file.Paths

public class NotificationTool extends AbstractAdminTool {

    private static final String DEFAULT_DESCRIPTION = "This tool is used to send notifications (bulletins) to a NiFi cluster. "
    private static final String HELP_ARG = "help"
    private static final String VERBOSE_ARG = "verbose"
    private static final String BOOTSTRAP_CONF = "bootstrapConf"
    private static final String PROXY_DN = "proxyDn"
    private static final String NIFI_INSTALL_DIR = "nifiInstallDir"
    private static final String NOTIFICATION_MESSAGE = "message"
    private static final String NOTIFICATION_LEVEL = "level"
    private final static String NOTIFICATION_ENDPOINT ="/nifi-api/controller/bulletin"
    private final static String SUPPORTED_MINIMUM_VERSION = "1.2.0"

    NotificationTool() {
        header = buildHeader(DEFAULT_DESCRIPTION)
        setup()
    }

    NotificationTool(final String description){
        header = buildHeader(description)
        setup()
    }

    @Override
    protected Logger getLogger() {
        LoggerFactory.getLogger(NotificationTool)
    }

    protected Options getOptions(){
        final Options options = new Options()
        options.addOption(Option.builder("h").longOpt(HELP_ARG).desc("Print help info").build())
        options.addOption(Option.builder("v").longOpt(VERBOSE_ARG).desc("Set mode to verbose (default is false)").build())
        options.addOption(Option.builder("p").longOpt(PROXY_DN).hasArg().desc("User or Proxy DN that has permission to send a notification. User must have view and modify privileges to 'access the controller' in NiFi").build())
        options.addOption(Option.builder("b").longOpt(BOOTSTRAP_CONF).hasArg().desc("Existing Bootstrap Configuration file").build())
        options.addOption(Option.builder("d").longOpt(NIFI_INSTALL_DIR).hasArg().desc("NiFi Installation Directory").build())
        options.addOption(Option.builder("m").longOpt(NOTIFICATION_MESSAGE).hasArg().desc("Notification message for nifi instance or cluster").build())
        options.addOption(Option.builder("l").longOpt(NOTIFICATION_LEVEL).required(false).hasArg().desc("Level for notification bulletin INFO,WARN,ERROR").build())
        options
    }

    void notifyCluster(final ClientFactory clientFactory, final String nifiPropertiesFile, final String bootstrapConfFile, final String nifiInstallDir, final String message, final String level, final String proxyDN){

        if(isVerbose){
            logger.info("Loading nifi properties for host information")
        }

        final String key = NiFiBootstrapUtils.extractKeyFromBootstrapFile(bootstrapConfFile)
        final NiFiProperties niFiProperties = NiFiPropertiesLoader.withKey(key).load(nifiPropertiesFile)
        final Client client =  clientFactory.getClient(niFiProperties,nifiInstallDir)
        final String url = NiFiClientUtil.getUrl(niFiProperties,NOTIFICATION_ENDPOINT)
        final WebTarget webTarget = client.target(url)

        if(isVerbose){
            logger.info("Contacting node at url:" + url)
        }

        final BulletinEntity bulletinEntity = new BulletinEntity()
        final BulletinDTO bulletinDTO = new BulletinDTO()
        bulletinDTO.message = message
        bulletinDTO.category = "NOTICE"
        bulletinDTO.level = StringUtils.isEmpty(level) ? "INFO" : level
        bulletinEntity.bulletin = bulletinDTO

        Response response
        if(!org.apache.nifi.util.StringUtils.isEmpty(niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT))) {

            if(StringUtils.isEmpty(proxyDN)){
                throw new UnsupportedOperationException("Proxy DN is required for sending a notification to this node or cluster")
            }

            response = webTarget.request().header(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN, ProxiedEntitiesUtils.formatProxyDn(proxyDN)).post(Entity.json(bulletinEntity))
        }
        else {
            response = webTarget.request().post(Entity.json(bulletinEntity))
        }

        Integer status = response.getStatus()

        if(status != 200){
            if(status == 404){
                throw new RuntimeException("The notification feature is not supported by each node in the cluster")
            }else{
                throw new RuntimeException("Failed with HTTP error code " + status + " with reason: " + response.readEntity(String.class))
            }
        }

    }

    void parse(final ClientFactory clientFactory, final String[] args) throws ParseException, UnsupportedOperationException {

        final CommandLine commandLine = new DefaultParser().parse(options,args)

        if (commandLine.hasOption(HELP_ARG)){
            printUsage(null)
        }else{

            if(commandLine.hasOption(BOOTSTRAP_CONF) && commandLine.hasOption(NOTIFICATION_MESSAGE) && commandLine.hasOption(NIFI_INSTALL_DIR)) {

                if(commandLine.hasOption(VERBOSE_ARG)){
                    this.isVerbose = true
                }

                final String bootstrapConfFileName = commandLine.getOptionValue(BOOTSTRAP_CONF)
                final File bootstrapConf = new File(bootstrapConfFileName)
                final Properties bootstrapProperties = AdminUtil.getBootstrapConf(Paths.get(bootstrapConfFileName))
                final String proxyDN = commandLine.getOptionValue(PROXY_DN)
                final String parentPathName = bootstrapConf.getCanonicalFile().getParentFile().getParentFile().getCanonicalPath()
                final String nifiConfDir = AdminUtil.getRelativeDirectory(bootstrapProperties.getProperty("conf.dir"),parentPathName)
                final String nifiLibDir = AdminUtil.getRelativeDirectory(bootstrapProperties.getProperty("lib.dir"),parentPathName)
                final String nifiPropertiesFileName = nifiConfDir + File.separator +"nifi.properties"
                final String notificationMessage = commandLine.getOptionValue(NOTIFICATION_MESSAGE)
                final String notificationLevel = commandLine.getOptionValue(NOTIFICATION_LEVEL)
                final String nifiInstallDir = commandLine.getOptionValue(NIFI_INSTALL_DIR)

                if(AdminUtil.supportedNiFiMinimumVersion(nifiConfDir, nifiLibDir, SUPPORTED_MINIMUM_VERSION)){
                    if(isVerbose){
                        logger.info("Attempting to connect with nifi using properties:", nifiPropertiesFileName)
                    }

                    notifyCluster(clientFactory, nifiPropertiesFileName, bootstrapConfFileName,nifiInstallDir,notificationMessage,notificationLevel,proxyDN)

                    if(isVerbose) {
                        logger.info("Message sent successfully to NiFi.")
                    }
                }else{
                    throw new UnsupportedOperationException("Notification Tool only supports NiFi versions 1.2.0 and above")
                }

            }else if(!commandLine.hasOption(BOOTSTRAP_CONF)){
                throw new ParseException("Missing -b option")
            }else if(!commandLine.hasOption(NIFI_INSTALL_DIR)){
                throw new ParseException("Missing -d option")
            }else{
                throw new ParseException("Missing -m option")
            }
        }

    }

    public static void main(String[] args) {
        final NotificationTool tool = new NotificationTool()
        final ClientFactory clientFactory = new NiFiClientFactory()

        try{
            tool.parse(clientFactory,args)
        } catch (Exception e) {
            tool.printUsage(e.message)
            System.exit(1)
        }

        System.exit(0)
    }


}
