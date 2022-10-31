package org.apache.nifi.snmp.factory.core;

import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.security.SecurityLevel;

import java.net.BindException;
import java.util.function.Function;

import static org.apache.nifi.snmp.helper.configurations.SNMPConfigurationFactory.LOCALHOST;
import static org.apache.nifi.snmp.helper.configurations.SNMPV3ConfigurationFactory.AUTH_PASSPHRASE;
import static org.apache.nifi.snmp.helper.configurations.SNMPV3ConfigurationFactory.AUTH_PROTOCOL;
import static org.apache.nifi.snmp.helper.configurations.SNMPV3ConfigurationFactory.PRIV_PASSPHRASE;
import static org.apache.nifi.snmp.helper.configurations.SNMPV3ConfigurationFactory.PRIV_PROTOCOL;
import static org.apache.nifi.snmp.helper.configurations.SNMPV3ConfigurationFactory.SECURITY_NAME;

public class SNMPTtest {

    protected static final int RETRIES = 3;

    protected SNMPConfiguration getSnmpConfiguration(int managerPort, String targetPort) {
        return new SNMPConfiguration.Builder()
                .setRetries(RETRIES)
                .setManagerPort(managerPort)
                .setTargetHost(LOCALHOST)
                .setTargetPort(targetPort)
                .setSecurityLevel(SecurityLevel.authPriv.name())
                .setSecurityName(SECURITY_NAME)
                .setAuthProtocol(AUTH_PROTOCOL)
                .setAuthPassphrase(AUTH_PASSPHRASE)
                .setPrivacyProtocol(PRIV_PROTOCOL)
                .setPrivacyPassphrase(PRIV_PASSPHRASE)
                .build();
    }

    protected Snmp createSnmpManagerInstance(final Function<SNMPConfiguration, Snmp> runnable, final int retries) throws BindException {
        int attempts = 0;
        while(attempts < retries) {
            try {
                return runnable.apply(getSnmpConfiguration(NetworkUtils.getAvailableUdpPort(), String.valueOf(NetworkUtils.getAvailableUdpPort())));
            } catch (Exception e) {
                if (e instanceof BindException) {
                    attempts++;
                }
            }
        }
        throw new BindException();
    }

    protected Target createTargetInstance(final Function<SNMPConfiguration, Target> runnable, final int retries) throws BindException {
        int attempts = 0;
        while(attempts < retries) {
            try {
                return runnable.apply(getSnmpConfiguration(NetworkUtils.getAvailableUdpPort(), String.valueOf(NetworkUtils.getAvailableUdpPort())));
            } catch (Exception e) {
                if (e instanceof BindException) {
                    attempts++;
                }
            }
        }
        throw new BindException();
    }
}
