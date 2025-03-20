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

import { Component, EventEmitter, Inject, Input, Output } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { ReactiveFormsModule } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe } from '@angular/common';
import { Observable } from 'rxjs';
import { DocumentedType } from '../../../../../state/shared';
import { CreateRegistryClientDialogRequest, CreateRegistryClientRequest } from '../../../state/registry-clients';
import { Client } from '../../../../../service/client.service';
import { MatSelectModule } from '@angular/material/select';
import { NiFiCommon, TextTip } from '@nifi/shared';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';
import { ExtensionCreation } from '../../../../../ui/common/extension-creation/extension-creation.component';

@Component({
    selector: 'create-registry-client',
    templateUrl: './create-registry-client.component.html',
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        AsyncPipe,
        MatSelectModule,
        ExtensionCreation
    ],
    styleUrls: ['./create-registry-client.component.scss']
})
export class CreateRegistryClient {
    @Input() saving$!: Observable<boolean>;
    @Output() createRegistryClient: EventEmitter<CreateRegistryClientRequest> =
        new EventEmitter<CreateRegistryClientRequest>();

    protected readonly TextTip = TextTip;

    registryClientTypes: DocumentedType[];

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: CreateRegistryClientDialogRequest,
        private nifiCommon: NiFiCommon,
        private client: Client,
        private clusterConnectionService: ClusterConnectionService
    ) {
        this.registryClientTypes = request.registryClientTypes;
    }

    registryClientTypeSelected(registryClientType: DocumentedType) {
        const request: CreateRegistryClientRequest = {
            revision: {
                clientId: this.client.getClientId(),
                version: 0
            },
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                name: this.nifiCommon.getComponentTypeLabel(registryClientType.type),
                type: registryClientType.type,
                bundle: registryClientType.bundle
            }
        };

        this.createRegistryClient.next(request);
    }
}
