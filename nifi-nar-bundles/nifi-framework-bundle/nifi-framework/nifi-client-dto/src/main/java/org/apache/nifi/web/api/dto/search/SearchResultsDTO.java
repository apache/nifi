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
package org.apache.nifi.web.api.dto.search;

import com.wordnik.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlType;

/**
 * The results of a search on this NiFi.
 */
@XmlType(name = "searchResults")
public class SearchResultsDTO {

    private List<ComponentSearchResultDTO> processorResults = new ArrayList<>();
    private List<ComponentSearchResultDTO> connectionResults = new ArrayList<>();
    private List<ComponentSearchResultDTO> processGroupResults = new ArrayList<>();
    private List<ComponentSearchResultDTO> inputPortResults = new ArrayList<>();
    private List<ComponentSearchResultDTO> outputPortResults = new ArrayList<>();
    private List<ComponentSearchResultDTO> remoteProcessGroupResults = new ArrayList<>();
    private List<ComponentSearchResultDTO> funnelResults = new ArrayList<>();

    /**
     * @return The processors that matched the search
     */
    @ApiModelProperty(
            value = "The processors that matched the search."
    )
    public List<ComponentSearchResultDTO> getProcessorResults() {
        return processorResults;
    }

    public void setProcessorResults(List<ComponentSearchResultDTO> processorResults) {
        this.processorResults = processorResults;
    }

    /**
     * @return connections that matched the search
     */
    @ApiModelProperty(
            value = "The connections that matched the search."
    )
    public List<ComponentSearchResultDTO> getConnectionResults() {
        return connectionResults;
    }

    public void setConnectionResults(List<ComponentSearchResultDTO> connectionResults) {
        this.connectionResults = connectionResults;
    }

    /**
     * @return process group that matched the search
     */
    @ApiModelProperty(
            value = "The process groups that matched the search."
    )
    public List<ComponentSearchResultDTO> getProcessGroupResults() {
        return processGroupResults;
    }

    public void setProcessGroupResults(List<ComponentSearchResultDTO> processGroupResults) {
        this.processGroupResults = processGroupResults;
    }

    /**
     * @return input ports that matched the search
     */
    @ApiModelProperty(
            value = "The input ports that matched the search."
    )
    public List<ComponentSearchResultDTO> getInputPortResults() {
        return inputPortResults;
    }

    /**
     * @return output ports that matched the search
     */
    @ApiModelProperty(
            value = "The output ports that matched the search."
    )
    public List<ComponentSearchResultDTO> getOutputPortResults() {
        return outputPortResults;
    }

    public void setInputPortResults(List<ComponentSearchResultDTO> inputPortResults) {
        this.inputPortResults = inputPortResults;
    }

    public void setOutputPortResults(List<ComponentSearchResultDTO> outputPortResults) {
        this.outputPortResults = outputPortResults;
    }

    /**
     * @return remote process groups that matched the search
     */
    @ApiModelProperty(
            value = "The remote process groups that matched the search."
    )
    public List<ComponentSearchResultDTO> getRemoteProcessGroupResults() {
        return remoteProcessGroupResults;
    }

    public void setRemoteProcessGroupResults(List<ComponentSearchResultDTO> remoteProcessGroupResults) {
        this.remoteProcessGroupResults = remoteProcessGroupResults;
    }

    /**
     * @return funnels that matched the search
     */
    @ApiModelProperty(
            value = "The funnels that matched the search."
    )
    public List<ComponentSearchResultDTO> getFunnelResults() {
        return funnelResults;
    }

    public void setFunnelResults(List<ComponentSearchResultDTO> funnelResults) {
        this.funnelResults = funnelResults;
    }

}
