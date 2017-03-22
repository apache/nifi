<%--
 Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
--%>
<%@ page contentType="text/html" pageEncoding="UTF-8" session="false" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<div id="system-diagnostics-dialog" class="hidden large-dialog">
    <div class="dialog-content">
        <div id="system-diagnostics-tabs" class="tab-container"></div>
        <div id="system-diagnostics-tabs-content">
            <div id="jvm-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <input type="hidden" id=""/>
                        <div class="setting-header"><fmt:message key="partials.summary.system-diagnostics-dialog.Heap"/> <span id="utilization-heap"></span></div>
                        <div class="setting-field">
                            <table id="heap-table">
                                <tbody>
                                <tr>
                                    <td class="memory-header setting-name"><fmt:message key="partials.summary.system-diagnostics-dialog.Max"/>:</td>
                                </tr>
                                <tr>
                                    <td><span id="max-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                <tr>
                                    <td class="setting-name"><fmt:message key="partials.summary.system-diagnostics-dialog.Total"/>:</td>
                                </tr>
                                <tr>
                                    <td><span id="total-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                <tr>
                                    <td class="setting-name"><fmt:message key="partials.summary.system-diagnostics-dialog.Used"/>:</td>
                                </tr>
                                <tr>
                                    <td><span id="used-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                <tr>
                                    <td class="setting-name"><fmt:message key="partials.summary.system-diagnostics-dialog.Free"/>:</td>
                                </tr>
                                <tr>
                                    <td><span id="free-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                </tbody>
                            </table>
                        </div>
                        <div class="clear"></div>
                    </div>
                </div>
                <div class="spacer"></div>
                <div class="settings-right">
                    <div class="setting">
                        <div class="setting-header"><fmt:message key="partials.summary.system-diagnostics-dialog.NonHeap"/> <span id="utilization-non-heap"></span></div>
                        <div class="setting-field">
                            <table id="heap-table">
                                <tbody>
                                <tr>
                                    <td class="memory-header setting-name"><fmt:message key="partials.summary.system-diagnostics-dialog.Max"/>:</td>
                                </tr>
                                <tr>
                                    <td><span id="max-non-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                <tr>
                                    <td class="setting-name"><fmt:message key="partials.summary.system-diagnostics-dialog.Total"/>:</td>
                                </tr>
                                <tr>
                                    <td><span id="total-non-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                <tr>
                                    <td class="setting-name"><fmt:message key="partials.summary.system-diagnostics-dialog.Used"/>:</td>
                                </tr>
                                <tr>
                                    <td><span id="used-non-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                <tr>
                                    <td class="setting-name"><fmt:message key="partials.summary.system-diagnostics-dialog.Free"/>:</td>
                                </tr>
                                <tr>
                                    <td><span id="free-non-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                </tbody>
                            </table>
                        </div>
                        <div class="clear"></div>
                    </div>
                </div>
                <div class="clear"></div>
                <div class="setting">
                    <div class="setting-header"><fmt:message key="partials.summary.system-diagnostics-dialog.GarbageCollection"/></div>
                    <div id="garbage-collection-container" class="setting-field">
                        <table id="garbage-collection-table">
                            <tbody></tbody>
                        </table>
                    </div>
                </div>
                <div class="setting">
                    <div class="setting-header"><fmt:message key="partials.summary.system-diagnostics-dialog.Runtime"/></div>
                    <div id="jvm-runtime-container" class="setting-field">
                        <table id="jvm-runtime-table">
                            <tbody>
                                <tr>
                                    <td class="setting-name"><fmt:message key="partials.summary.system-diagnostics-dialog.Uptime"/>:</td>
                                </tr>
                                <tr>
                                    <td><span id="uptime"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
            <div id="system-tab-content"class="configuration-tab">
            <div class="settings-left">
                <div class="setting">
                    <div class="setting-name"><fmt:message key="partials.summary.system-diagnostics-dialog.AvailableProcessors"/>:</div>
                    <div class="setting-field">
                        <div id="available-processors"></div>
                    </div>
                </div>
            </div>
            <div class="spacer"></div>
            <div class="settings-right">
                <div class="setting">
                    <div class="setting-name">
                        <fmt:message key="partials.summary.system-diagnostics-dialog.ProcessorLoadAverage"/>:
                        <fmt:message key="partials.summary.system-diagnostics-dialog.ProcessorLoadAverage.title" var="system-diagnostics-dialog_ProcessorLoadAverage"/>
                        <div class="fa fa-question-circle" alt="Info" title="${system-diagnostics-dialog_ProcessorLoadAverage}"></div>
                    </div>
                    <div class="setting-field">
                        <div id="processor-load-average"></div>
                    </div>
                </div>
            </div>
            <div class="clear"></div>
            <div class="setting">
                <div class="setting-header"><fmt:message key="partials.summary.system-diagnostics-dialog.FlowFileRepositoryStorageUsage"/></div>
                <div class="setting-field">
                    <div id="flow-file-repository-storage-usage-container"></div>
                </div>
            </div>
            <div class="setting">
                <div class="setting-header"><fmt:message key="partials.summary.system-diagnostics-dialog.ContentRepositoryStorageUsage"/></div>
                <div class="setting-field">
                    <div id="content-repository-storage-usage-container"></div>
                </div>
            </div>
        </div>

        <div id="version-tab-content" class="configuration-tab">
            <div class="setting">
                <div class="setting-header"><fmt:message key="partials.summary.version-tab-content.nifi"/></div>
                <dl class="setting-attributes-list">
                    <dt><fmt:message key="partials.summary.version-tab-content.nifiVersion"/></dt><dd><span id="version-nifi"></span></dd>
                    <dt><fmt:message key="partials.summary.version-tab-content.tag"/></dt><dd><span id="version-build-tag"></span></dd>
                    <dt><fmt:message key="partials.summary.version-tab-content.buildDate"/></dt><dd><span id="version-build-timestamp"></span></dd>
                    <dt><fmt:message key="partials.summary.version-tab-content.branch"/></dt><dd><span id="version-build-branch"></span></dd>
                    <dt><fmt:message key="partials.summary.version-tab-content.revision"/></dt><dd><span id="version-build-revision"></span></dd>
                </dl>
            </div>
            <div class="setting">
                <div class="setting-header">Java</div>
                <dl class="setting-attributes-list">
                    <dt><fmt:message key="partials.summary.version-tab-content.Version"/></dt><dd><span id="version-java-version"></span></dd>
                    <dt><fmt:message key="partials.summary.version-tab-content.Vendor"/></dt><dd><span id="version-java-vendor"></span></dd>
                </dl>
            </div>
            <div class="setting">
                <div class="setting-header"><fmt:message key="partials.summary.version-tab-content.Operating"/></div>
                <dl class="setting-attributes-list">
                    <dt><fmt:message key="partials.summary.version-tab-content.Name"/></dt><dd><span id="version-os-name"></span></dd>
                    <dt><fmt:message key="partials.summary.version-tab-content.Version"/></dt><dd><span id="version-os-version"></span></dd>
                    <dt><fmt:message key="partials.summary.version-tab-content.Architecture"/></dt><dd><span id="version-os-arch"></span></dd>
                </dl>
            </div>
        </div>

        </div>
        <div id="system-diagnostics-refresh-container">
        <fmt:message key="partials.summary.system-diagnostics-refresh-container.refresh" var="refresh"/>
            <button id="system-diagnostics-refresh-button" class="refresh-button pointer fa fa-refresh" title="${refresh}"></button>
            <div id="system-diagnostics-last-refreshed-container" class="last-refreshed-container">
                <span id="system-diagnostics-last-refreshed" class="value-color"></span>
            </div>
            <div id="system-diagnostics-loading-container" class="loading-container"></div>
        </div>
    </div>
</div>
