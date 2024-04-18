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
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { Store } from '@ngrx/store';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe } from '@angular/common';
import { ErrorBanner } from '../../../../../ui/common/error-banner/error-banner.component';
import { NifiSpinnerDirective } from '../../../../../ui/common/spinner/nifi-spinner.directive';
import { selectSaving } from '../../../state/manage-remote-ports/manage-remote-ports.selectors';
import { EditRemotePortDialogRequest } from '../../../state/flow';
import { Client } from '../../../../../service/client.service';
import { ComponentType } from '../../../../../state/shared';
import { PortSummary } from '../../../state/manage-remote-ports';
import { configureRemotePort } from '../../../state/manage-remote-ports/manage-remote-ports.actions';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';

@Component({
    standalone: true,
    templateUrl: './edit-remote-port.component.html',
    imports: [
        ReactiveFormsModule,
        ErrorBanner,
        MatDialogModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        AsyncPipe,
        NifiSpinnerDirective
    ],
    styleUrls: ['./edit-remote-port.component.scss']
})
export class EditRemotePortComponent {
    saving$ = this.store.select(selectSaving);

    editPortForm: FormGroup;
    portTypeLabel: string;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditRemotePortDialogRequest,
        private formBuilder: FormBuilder,
        private store: Store<CanvasState>,
        private client: Client,
        private clusterConnectionService: ClusterConnectionService
    ) {
        // set the port type name
        if (ComponentType.InputPort == this.request.type) {
            this.portTypeLabel = 'Input Port';
        } else {
            this.portTypeLabel = 'Output Port';
        }

        // build the form
        this.editPortForm = this.formBuilder.group({
            concurrentTasks: new FormControl(request.entity.concurrentlySchedulableTaskCount, Validators.required),
            compressed: new FormControl(request.entity.useCompression),
            count: new FormControl(request.entity.batchSettings.count),
            size: new FormControl(request.entity.batchSettings.size),
            duration: new FormControl(request.entity.batchSettings.duration)
        });
    }

    editRemotePort() {
        const payload: any = {
            revision: this.client.getRevision(this.request.rpg),
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            type: this.request.type,
            remoteProcessGroupPort: {
                concurrentlySchedulableTaskCount: this.editPortForm.get('concurrentTasks')?.value,
                useCompression: this.editPortForm.get('compressed')?.value,
                batchSettings: {
                    count: this.editPortForm.get('count')?.value,
                    size: this.editPortForm.get('size')?.value,
                    duration: this.editPortForm.get('duration')?.value
                },
                id: this.request.entity.id,
                groupId: this.request.entity.groupId
            } as PortSummary
        };

        this.store.dispatch(
            configureRemotePort({
                request: {
                    id: this.request.entity.id,
                    uri: this.request.rpg.uri,
                    payload
                }
            })
        );
    }
}
