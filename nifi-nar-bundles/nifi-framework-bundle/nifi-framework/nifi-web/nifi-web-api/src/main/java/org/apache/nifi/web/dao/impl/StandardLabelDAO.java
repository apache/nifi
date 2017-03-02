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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Size;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.dao.LabelDAO;

import java.util.Set;

public class StandardLabelDAO extends ComponentDAO implements LabelDAO {

    private FlowController flowController;

    private Label locateLabel(final String labelId) {
        final ProcessGroup rootGroup = flowController.getGroup(flowController.getRootGroupId());
        final Label label = rootGroup.findLabel(labelId);

        if (label == null) {
            throw new ResourceNotFoundException(String.format("Unable to find label with id '%s'.", labelId));
        } else {
            return label;
        }
    }

    @Override
    public boolean hasLabel(String labelId) {
        final ProcessGroup rootGroup = flowController.getGroup(flowController.getRootGroupId());
        return rootGroup.findLabel(labelId) != null;
    }

    @Override
    public Label createLabel(String groupId, LabelDTO labelDTO) {
        if (labelDTO.getParentGroupId() != null && !flowController.areGroupsSame(groupId, labelDTO.getParentGroupId())) {
            throw new IllegalArgumentException("Cannot specify a different Parent Group ID than the Group to which the Label is being added.");
        }

        // get the desired group
        ProcessGroup group = locateProcessGroup(flowController, groupId);

        // create the label
        Label label = flowController.createLabel(labelDTO.getId(), labelDTO.getLabel());
        if (labelDTO.getPosition() != null) {
            label.setPosition(new Position(labelDTO.getPosition().getX(), labelDTO.getPosition().getY()));
        }
        if (labelDTO.getWidth() != null && labelDTO.getHeight() != null) {
            label.setSize(new Size(labelDTO.getWidth(), labelDTO.getHeight()));
        }
        label.setStyle(labelDTO.getStyle());

        // add the label
        group.addLabel(label);
        return label;
    }

    @Override
    public Label getLabel(String labelId) {
        return locateLabel(labelId);
    }

    @Override
    public Set<Label> getLabels(String groupId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        return group.getLabels();
    }

    @Override
    public Label updateLabel(LabelDTO labelDTO) {
        // get the label being updated
        Label label = locateLabel(labelDTO.getId());

        // update the label state
        if (labelDTO.getPosition() != null) {
            label.setPosition(new Position(labelDTO.getPosition().getX(), labelDTO.getPosition().getY()));
        }
        if (labelDTO.getStyle() != null) {
            label.setStyle(labelDTO.getStyle());
        }
        if (labelDTO.getLabel() != null) {
            label.setValue(labelDTO.getLabel());
        }
        if (labelDTO.getWidth() != null && labelDTO.getHeight() != null) {
            label.setSize(new Size(labelDTO.getWidth(), labelDTO.getHeight()));
        }

        return label;
    }

    @Override
    public void deleteLabel(String labelId) {
        // get the label
        Label label = locateLabel(labelId);

        // remove the label
        label.getProcessGroup().removeLabel(label);
    }

    /* setters */
    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }
}
