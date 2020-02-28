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
package org.apache.nifi.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.nifi.security.krb.KerberosPasswordUser
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Shared
import spock.lang.Specification

class SecurityUtilITSpec extends Specification {

    @Shared
    private static Logger LOGGER
    @Shared
    private MiniKdc miniKdc
    @Shared
    private Configuration configuration

    def setupSpec() {
        LOGGER = LoggerFactory.getLogger SecurityUtilITSpec
        configuration = new Configuration()
        configuration.set CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos"
        configuration.setBoolean CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true
        configuration.setInt CommonConfigurationKeysPublic.HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN, 5
        def miniKdcConf = MiniKdc.createConf()
        miniKdcConf.setProperty MiniKdc.INSTANCE, InetAddress.localHost.hostName
        miniKdcConf.setProperty MiniKdc.ORG_NAME, "EXAMPLE"
        miniKdcConf.setProperty MiniKdc.ORG_DOMAIN, "COM"
        miniKdcConf.setProperty MiniKdc.DEBUG, "false"
        miniKdcConf.setProperty MiniKdc.MAX_TICKET_LIFETIME, "5"
        miniKdc = new MiniKdc(miniKdcConf, new File("./target/minikdc"))
        miniKdc.start()
    }

    def cleanupSpec() {
        miniKdc.stop()
    }

    def "getUgiForKerberosUser with unauthenticated KerberosPasswordUser returns correct UGI"() {
        given:
        def principal = "testprincipal2"
        def password = "password"
        miniKdc.createPrincipal principal, password

        when: "A KerberosPasswordUser is created for the given principal"
        def kerberosPasswordUser = new KerberosPasswordUser(principal, password)

        then: "The created KerberosPasswordUser is not logged in"
        !kerberosPasswordUser.isLoggedIn()

        when: "A UGI is acquired for the KerberosPasswordUser"
        def ugi = SecurityUtil.getUgiForKerberosUser configuration, kerberosPasswordUser

        then: "The KerberosPasswordUser is logged in and the acquired UGI is valid for the given principal"
        kerberosPasswordUser.isLoggedIn()
        ugi != null
        ugi.getShortUserName() == principal
        LOGGER.debug "UGI = [{}], KerberosUser = [{}]", ugi, kerberosPasswordUser
    }

    def "getUgiForKerberosUser with authenticated KerberosPasswordUser returns correct UGI"() {
        given:
        def principal = "testprincipal3"
        def password = "password"
        miniKdc.createPrincipal principal, password

        when: "A KerberosPasswordUser is created for the given principal and authenticated"
        def kerberosPasswordUser = new KerberosPasswordUser(principal, password)
        kerberosPasswordUser.login()

        then: "The created KerberosPasswordUser is logged in"
        kerberosPasswordUser.isLoggedIn()

        when: "A UGI is acquired for the KerberosPasswordUser"
        def ugi = SecurityUtil.getUgiForKerberosUser configuration, kerberosPasswordUser

        then: "The KerberosPasswordUser is logged in and the acquired UGI is valid for the given principal"
        kerberosPasswordUser.isLoggedIn()
        ugi != null
        ugi.getShortUserName() == principal
        LOGGER.debug "UGI = [{}], KerberosUser = [{}]", ugi, kerberosPasswordUser
    }
}
