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

import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { CanvasState } from '../../../state';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { Store } from '@ngrx/store';
import { updateComponent } from '../../../state/flow/flow.actions';
import { Client } from '../../../service/client.service';
import { EditComponent } from '../../../state/flow';
import { ComponentType } from '../../../state/shared';

@Component({
    selector: 'edit-port',
    templateUrl: './edit-port.component.html',
    styleUrls: ['./edit-port.component.scss']
})
export class EditPort {
    editPortForm: FormGroup;
    portTypeLabel: string;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditComponent,
        private formBuilder: FormBuilder,
        private store: Store<CanvasState>,
        private client: Client
    ) {
        // set the port type name
        if (ComponentType.InputPort == this.request.type) {
            this.portTypeLabel = 'Input Port';
        } else {
            this.portTypeLabel = 'Output Port';
        }

        // TODO - consider updating the request to only provide the id of the port and selecting that item
        // from the store. this would also allow us to be informed when another client has submitted an
        // update to the same port which this editing is happening

        // build the form
        this.editPortForm = this.formBuilder.group({
            name: new FormControl(request.entity.component.name, Validators.required),
            concurrentTasks: new FormControl(
                request.entity.component.concurrentlySchedulableTaskCount,
                Validators.required
            ),
            comments: new FormControl(request.entity.component.comments),
            portFunction: new FormControl(request.entity.component.portFunction == 'FAILURE')
        });
    }

    editPort() {
        const payload: any = {
            revision: this.client.getRevision(this.request.entity),
            component: {
                id: this.request.entity.id,
                name: this.editPortForm.get('name')?.value,
                comments: this.editPortForm.get('comments')?.value
            }
        };

        // if this port allows remote access update the concurrent tasks
        if (this.request.entity.component.allowRemoteAccess) {
            payload.component.concurrentlySchedulableTaskCount = this.editPortForm.get('concurrentTasks')?.value;
        }

        // if this port is an output port update the port function
        if (ComponentType.OutputPort == this.request.entity.type) {
            payload.component.portFunction = this.editPortForm.get('portFunction')?.value ? 'FAILURE' : 'STANDARD';
        }

        this.store.dispatch(
            updateComponent({
                request: {
                    id: this.request.entity.id,
                    type: this.request.type,
                    uri: this.request.uri,
                    payload
                }
            })
        );
    }
}
