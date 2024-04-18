/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, EventEmitter, Inject, Input, Output } from '@angular/core';

import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { MatTabsModule } from '@angular/material/tabs';
import { MatButtonModule } from '@angular/material/button';
import { NifiSpinnerDirective } from '../../../../../ui/common/spinner/nifi-spinner.directive';
import { Observable } from 'rxjs';
import {
    InlineServiceCreationRequest,
    InlineServiceCreationResponse,
    ParameterContextReferenceEntity,
    Property
} from '../../../../../state/shared';
import {
    EditParameterProviderRequest,
    ParameterProviderEntity,
    UpdateParameterProviderRequest
} from '../../../state/parameter-providers';
import { AbstractControl, FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { Client } from '../../../../../service/client.service';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { ControllerServiceReferences } from '../../../../../ui/common/controller-service/controller-service-references/controller-service-references.component';
import { ParameterProviderReferences } from '../parameter-context-references/parameter-provider-references.component';
import { PropertyTable } from '../../../../../ui/common/property-table/property-table.component';
import { ErrorBanner } from '../../../../../ui/common/error-banner/error-banner.component';
import { CommonModule } from '@angular/common';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';

@Component({
    selector: 'edit-parameter-provider',
    standalone: true,
    imports: [
        MatDialogModule,
        MatTabsModule,
        MatButtonModule,
        NifiSpinnerDirective,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatInputModule,
        ControllerServiceReferences,
        ParameterProviderReferences,
        PropertyTable,
        ErrorBanner,
        CommonModule
    ],
    templateUrl: './edit-parameter-provider.component.html',
    styleUrls: ['./edit-parameter-provider.component.scss']
})
export class EditParameterProvider {
    @Input() createNewProperty!: (existingProperties: string[], allowsSensitive: boolean) => Observable<Property>;
    @Input() createNewService!: (request: InlineServiceCreationRequest) => Observable<InlineServiceCreationResponse>;
    @Input() goToService!: (serviceId: string) => void;
    @Input() goToReferencingParameterContext!: (parameterContextId: string) => void;
    @Input() saving$!: Observable<boolean>;

    @Output() editParameterProvider: EventEmitter<UpdateParameterProviderRequest> =
        new EventEmitter<UpdateParameterProviderRequest>();

    editParameterProviderForm: FormGroup;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditParameterProviderRequest,
        private formBuilder: FormBuilder,
        private client: Client,
        private nifiCommon: NiFiCommon,
        private clusterConnectionService: ClusterConnectionService
    ) {
        const providerProperties = request.parameterProvider.component.properties;
        const properties: Property[] = Object.entries(providerProperties).map((entry: any) => {
            const [property, value] = entry;
            return {
                property,
                value,
                descriptor: request.parameterProvider.component.descriptors[property]
            };
        });

        // build the form
        this.editParameterProviderForm = this.formBuilder.group({
            name: new FormControl(request.parameterProvider.component.name, Validators.required),
            properties: new FormControl(properties),
            comments: new FormControl(request.parameterProvider.component.comments)
        });
    }

    formatType(entity: ParameterProviderEntity): string {
        return this.nifiCommon.formatType(entity.component);
    }

    formatBundle(entity: ParameterProviderEntity): string {
        return this.nifiCommon.formatBundle(entity.component.bundle);
    }

    submitForm(postUpdateNavigation?: string[]) {
        const payload: any = {
            revision: this.client.getRevision(this.request.parameterProvider),
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                id: this.request.parameterProvider.id,
                name: this.editParameterProviderForm.get('name')?.value,
                comments: this.editParameterProviderForm.get('comments')?.value
            }
        };

        const propertyControl: AbstractControl | null = this.editParameterProviderForm.get('properties');
        if (propertyControl && propertyControl.dirty) {
            const properties: Property[] = propertyControl.value;
            const values: { [key: string]: string | null } = {};
            properties.forEach((property) => (values[property.property] = property.value));
            payload.component.properties = values;
            payload.component.sensitiveDynamicPropertyNames = properties
                .filter((property) => property.descriptor.dynamic && property.descriptor.sensitive)
                .map((property) => property.descriptor.name);
        }

        this.editParameterProvider.next({
            payload,
            postUpdateNavigation
        });
    }

    navigateToParameterContext(parameterContextReference: ParameterContextReferenceEntity) {
        if (parameterContextReference.component?.id) {
            this.goToReferencingParameterContext(parameterContextReference.component?.id);
        }
    }
}
