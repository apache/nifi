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

package org.apache.nifi.minifi.bootstrap.command;

import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.CMD_LOGGER;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.DEFAULT_LOGGER;
import static org.apache.nifi.minifi.bootstrap.Status.ERROR;
import static org.apache.nifi.minifi.bootstrap.Status.MINIFI_NOT_RUNNING;
import static org.apache.nifi.minifi.bootstrap.Status.OK;

import java.lang.reflect.Method;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.nifi.minifi.bootstrap.MiNiFiParameters;
import org.apache.nifi.minifi.bootstrap.MiNiFiStatus;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiStatusProvider;

public class EnvRunner implements CommandRunner {
    private final MiNiFiParameters miNiFiParameters;
    private final MiNiFiStatusProvider miNiFiStatusProvider;

    public EnvRunner(MiNiFiParameters miNiFiParameters, MiNiFiStatusProvider miNiFiStatusProvider) {
        this.miNiFiParameters = miNiFiParameters;
        this.miNiFiStatusProvider = miNiFiStatusProvider;
    }

    /**
     * Returns information about the MiNiFi's virtual machine.
     * @param args the input arguments
     * @return status code
     */
    @Override
    public int runCommand(String[] args) {
        return env();
    }

    private int env() {
        MiNiFiStatus status = miNiFiStatusProvider.getStatus(miNiFiParameters.getMiNiFiPort(), miNiFiParameters.getMinifiPid());
        if (status.getPid() == null) {
            CMD_LOGGER.error("Apache MiNiFi is not running");
            return MINIFI_NOT_RUNNING.getStatusCode();
        }

        Class<?> virtualMachineClass;
        try {
            virtualMachineClass = Class.forName("com.sun.tools.attach.VirtualMachine");
        } catch (ClassNotFoundException cnfe) {
            CMD_LOGGER.error("Seems tools.jar (Linux / Windows JDK) or classes.jar (Mac OS) is not available in classpath");
            return ERROR.getStatusCode();
        }

        Method attachMethod;
        Method detachMethod;
        try {
            attachMethod = virtualMachineClass.getMethod("attach", String.class);
            detachMethod = virtualMachineClass.getDeclaredMethod("detach");
        } catch (Exception e) {
            CMD_LOGGER.error("Methods required for getting environment not available");
            DEFAULT_LOGGER.error("Exception:", e);
            return ERROR.getStatusCode();
        }

        Object virtualMachine;
        try {
            virtualMachine = attachMethod.invoke(null, status.getPid());
        } catch (Exception e) {
            CMD_LOGGER.error("Problem attaching to MiNiFi");
            DEFAULT_LOGGER.error("Exception:", e);
            return ERROR.getStatusCode();
        }

        try {
            Method getSystemPropertiesMethod = virtualMachine.getClass().getMethod("getSystemProperties");

            Properties sysProps = (Properties) getSystemPropertiesMethod.invoke(virtualMachine);
            for (Entry<Object, Object> syspropEntry : sysProps.entrySet()) {
                CMD_LOGGER.info(syspropEntry.getKey().toString() + " = " + syspropEntry.getValue().toString());
            }
        } catch (Exception e) {
            CMD_LOGGER.error("Exception happened during the systemproperties call");
            DEFAULT_LOGGER.error("Exception:", e);
            return ERROR.getStatusCode();
        } finally {
            try {
                detachMethod.invoke(virtualMachine);
            } catch (final Exception e) {
                CMD_LOGGER.warn("Caught exception detaching from process", e);
            }
        }
        return OK.getStatusCode();
    }
}
