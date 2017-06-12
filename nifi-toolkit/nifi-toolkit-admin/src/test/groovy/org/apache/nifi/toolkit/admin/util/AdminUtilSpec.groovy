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
package org.apache.nifi.toolkit.admin.util

import spock.lang.Specification

class AdminUtilSpec extends Specification{

    def "get nifi version with version in properties"(){

        setup:

        def nifiConfDir = new File("src/test/resources/conf")
        def nifiLibDir = new File("src/test/resources/lib")

        when:

        def version = AdminUtil.getNiFiVersion(nifiConfDir,nifiLibDir)

        then:
        version == "1.1.0"
    }

    def "get nifi version with version in nar"(){

        setup:

        def nifiConfDir = new File("src/test/resources/upgrade/conf")
        def nifiLibDir = new File("src/test/resources/lib")

        when:

        def version = AdminUtil.getNiFiVersion(nifiConfDir,nifiLibDir)

        then:
        version == "1.2.0"
    }


    def "get bootstrap properties"(){

        given:

        def bootstrapConf = new File("src/test/resources/conf/bootstrap.conf")

        when:

        def properties = AdminUtil.getBootstrapConf(bootstrapConf.toPath())

        then:
        properties.get("conf.dir") == "./conf"

    }

    def "supported version should be true"(){

        expect:
        AdminUtil.supportedVersion("1.0.0","1.2.0","1.1.0")

    }

    def "supported version should be false"(){

        expect:
        !AdminUtil.supportedVersion("1.0.0","1.2.0","1.3.0")

    }



}
