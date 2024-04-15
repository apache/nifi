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
import { AbstractControl, FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe } from '@angular/common';
import { Observable } from 'rxjs';
import {
    DocumentedType,
    InlineServiceCreationRequest,
    InlineServiceCreationResponse,
    Parameter,
    Property,
    TextTipInput
} from '../../../../../state/shared';
import { EditRegistryClientDialogRequest, EditRegistryClientRequest } from '../../../state/registry-clients';
import { NifiSpinnerDirective } from '../../../../../ui/common/spinner/nifi-spinner.directive';
import { Client } from '../../../../../service/client.service';
import { MatSelectModule } from '@angular/material/select';
import { NifiTooltipDirective } from '../../../../../ui/common/tooltips/nifi-tooltip.directive';
import { TextTip } from '../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { MatTabsModule } from '@angular/material/tabs';
import { PropertyTable } from '../../../../../ui/common/property-table/property-table.component';
import { ErrorBanner } from '../../../../../ui/common/error-banner/error-banner.component';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';

@Component({
    selector: 'edit-registry-client',
    standalone: true,
    templateUrl: './edit-registry-client.component.html',
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        AsyncPipe,
        NifiSpinnerDirective,
        MatSelectModule,
        NifiTooltipDirective,
        MatTabsModule,
        PropertyTable,
        ErrorBanner
    ],
    styleUrls: ['./edit-registry-client.component.scss']
})
export class EditRegistryClient {
    @Input() createNewProperty!: (existingProperties: string[], allowsSensitive: boolean) => Observable<Property>;
    @Input() createNewService!: (request: InlineServiceCreationRequest) => Observable<InlineServiceCreationResponse>;
    @Input() getParameters!: (sensitive: boolean) => Observable<Parameter[]>;
    @Input() goToService!: (serviceId: string) => void;
    @Input() saving$!: Observable<boolean>;
    @Output() editRegistryClient: EventEmitter<EditRegistryClientRequest> =
        new EventEmitter<EditRegistryClientRequest>();

    protected readonly TextTip = TextTip;

    editRegistryClientForm: FormGroup;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditRegistryClientDialogRequest,
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon,
        private client: Client,
        private clusterConnectionService: ClusterConnectionService
    ) {
        const serviceProperties: any = request.registryClient.component.properties;
        const properties: Property[] = Object.entries(serviceProperties).map((entry: any) => {
            const [property, value] = entry;
            return {
                property,
                value,
                descriptor: request.registryClient.component.descriptors[property]
            };
        });

        // build the form
        this.editRegistryClientForm = this.formBuilder.group({
            name: new FormControl(request.registryClient.component.name, Validators.required),
            description: new FormControl(request.registryClient.component.description),
            properties: new FormControl(properties)
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

    submitForm(postUpdateNavigation?: string[]) {
        const payload: any = {
            revision: this.client.getRevision(this.request.registryClient),
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                id: this.request.registryClient.id,
                name: this.editRegistryClientForm.get('name')?.value,
                type: this.editRegistryClientForm.get('type')?.value,
                description: this.editRegistryClientForm.get('description')?.value
            }
        };

        const propertyControl: AbstractControl | null = this.editRegistryClientForm.get('properties');
        if (propertyControl && propertyControl.dirty) {
            const properties: Property[] = propertyControl.value;
            const values: { [key: string]: string | null } = {};
            properties.forEach((property) => (values[property.property] = property.value));
            payload.component.properties = values;
            payload.component.sensitiveDynamicPropertyNames = properties
                .filter((property) => property.descriptor.dynamic && property.descriptor.sensitive)
                .map((property) => property.descriptor.name);
        }

        this.editRegistryClient.next({
            id: this.request.registryClient.id,
            uri: this.request.registryClient.uri,
            payload,
            postUpdateNavigation
        });
    }
}
