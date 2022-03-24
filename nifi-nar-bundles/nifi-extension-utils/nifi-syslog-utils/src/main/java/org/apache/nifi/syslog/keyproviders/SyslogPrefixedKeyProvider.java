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

package org.apache.nifi.syslog.keyproviders;

import com.github.palindromicity.syslog.KeyProvider;
import org.apache.nifi.syslog.attributes.Syslog5424Attributes;
import org.apache.nifi.syslog.attributes.SyslogAttributes;

import java.util.regex.Pattern;

public class SyslogPrefixedKeyProvider implements KeyProvider {
    private Pattern pattern;

    public SyslogPrefixedKeyProvider() {
    }

    @Override
    public String getMessage() {
        return SyslogAttributes.SYSLOG_BODY.key();
    }

    @Override
    public String getHeaderAppName() {
        return Syslog5424Attributes.SYSLOG_APP_NAME.key();
    }

    @Override
    public String getHeaderHostName() {
        return SyslogAttributes.SYSLOG_HOSTNAME.key();
    }

    @Override
    public String getHeaderPriority() {
        return SyslogAttributes.SYSLOG_PRIORITY.key();
    }

    @Override
    public String getHeaderFacility() {
        return SyslogAttributes.SYSLOG_FACILITY.key();
    }

    @Override
    public String getHeaderSeverity() {
        return SyslogAttributes.SYSLOG_SEVERITY.key();
    }


    @Override
    public String getHeaderProcessId() {
        return Syslog5424Attributes.SYSLOG_PROCID.key();
    }

    @Override
    public String getHeaderTimeStamp() {
        return SyslogAttributes.SYSLOG_TIMESTAMP.key();
    }

    @Override
    public String getHeaderMessageId() {
        return Syslog5424Attributes.SYSLOG_MESSAGEID.key();
    }

    @Override
    public String getHeaderVersion() {
        return SyslogAttributes.SYSLOG_VERSION.key();
    }

    @Override
    public String getStructuredBase() {
        return Syslog5424Attributes.SYSLOG_STRUCTURED_BASE.key();
    }

    @Override
    public String getStructuredElementIdFormat() {
        return Syslog5424Attributes.SYSLOG_STRUCTURED_ELEMENT_ID_FMT.key();
    }

    @Override
    public String getStructuredElementIdParamNameFormat() {
        return Syslog5424Attributes.SYSLOG_STRUCTURED_ELEMENT_ID_PNAME_FMT.key();
    }

    @Override
    public Pattern getStructuredElementIdParamNamePattern() {
        if (pattern == null) {
            pattern = Pattern.compile(Syslog5424Attributes.SYSLOG_STRUCTURED_ELEMENT_ID_PNAME_PATTERN.key());
        }
        return pattern;
    }
}
