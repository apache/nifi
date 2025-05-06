package org.apache.nifi.web.api.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.util.List;

@XmlRootElement(name = "processGroupOptionsEntity")
public class ProcessGroupOptionsEntity extends Entity {
    private List<ProcessGroupOptionEntity> processGroupOptionEntities;

    @Schema(description = "The list of ProcessGroupOptionEntities.")
    public List<ProcessGroupOptionEntity> getProcessGroupOptionEntities() {
        return processGroupOptionEntities;
    }

    public void setProcessGroupOptionEntities(List<ProcessGroupOptionEntity> processGroupOptionEntities) {
        this.processGroupOptionEntities = processGroupOptionEntities;
    }
}
