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
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe } from '@angular/common';
import { Observable } from 'rxjs';
import { DocumentedType, TextTipInput } from '../../../../../state/shared';
import { CreateRegistryClientDialogRequest, CreateRegistryClientRequest } from '../../../state/registry-clients';
import { NifiSpinnerDirective } from '../../../../../ui/common/spinner/nifi-spinner.directive';
import { Client } from '../../../../../service/client.service';
import { MatSelectModule } from '@angular/material/select';
import { NifiTooltipDirective } from '../../../../../ui/common/tooltips/nifi-tooltip.directive';
import { TextTip } from '../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';

@Component({
    selector: 'create-registry-client',
    standalone: true,
    templateUrl: './create-registry-client.component.html',
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        AsyncPipe,
        NifiSpinnerDirective,
        MatSelectModule,
        NifiTooltipDirective
    ],
    styleUrls: ['./create-registry-client.component.scss']
})
export class CreateRegistryClient {
    @Input() saving$!: Observable<boolean>;
    @Output() createRegistryClient: EventEmitter<CreateRegistryClientRequest> =
        new EventEmitter<CreateRegistryClientRequest>();

    protected readonly TextTip = TextTip;

    createRegistryClientForm: FormGroup;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: CreateRegistryClientDialogRequest,
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon,
        private client: Client,
        private clusterConnectionService: ClusterConnectionService
    ) {
        let type: string | null = null;
        if (request.registryClientTypes.length > 0) {
            type = request.registryClientTypes[0].type;
        }

        // build the form
        this.createRegistryClientForm = this.formBuilder.group({
            name: new FormControl('', Validators.required),
            type: new FormControl(type, Validators.required),
            description: new FormControl('')
        });
    }

    formatType(option: DocumentedType): string {
        return this.nifiCommon.substringAfterLast(option.type, '.');
    }

    getOptionTipData(option: DocumentedType): TextTipInput {
        return {
            // @ts-ignore
            text: option.description
        };
    }

    createRegistryClientClicked() {
        const request: CreateRegistryClientRequest = {
            revision: {
                clientId: this.client.getClientId(),
                version: 0
            },
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                name: this.createRegistryClientForm.get('name')?.value,
                type: this.createRegistryClientForm.get('type')?.value,
                description: this.createRegistryClientForm.get('description')?.value
            }
        };

        this.createRegistryClient.next(request);
    }
}
