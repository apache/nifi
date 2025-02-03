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
import { Observable, of } from 'rxjs';
import { InlineServiceCreationRequest, InlineServiceCreationResponse, Property } from '../../../../../state/shared';
import {
    EditParameterProviderRequest,
    ParameterProviderEntity,
    UpdateParameterProviderRequest
} from '../../../state/parameter-providers';
import { AbstractControl, FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { Client } from '../../../../../service/client.service';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { ParameterProviderReferences } from '../parameter-context-references/parameter-provider-references.component';
import { PropertyTable } from '../../../../../ui/common/property-table/property-table.component';
import { CommonModule } from '@angular/common';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';
import {
    TextTip,
    NiFiCommon,
    NifiTooltipDirective,
    CopyDirective,
    ParameterContextReferenceEntity
} from '@nifi/shared';
import {
    ConfigVerificationResult,
    ModifiedProperties,
    VerifyPropertiesRequestContext
} from '../../../../../state/property-verification';
import { PropertyVerification } from '../../../../../ui/common/property-verification/property-verification.component';
import { TabbedDialog } from '../../../../../ui/common/tabbed-dialog/tabbed-dialog.component';
import { ErrorContextKey } from '../../../../../state/error';
import { ContextErrorBanner } from '../../../../../ui/common/context-error-banner/context-error-banner.component';

@Component({
    selector: 'edit-parameter-provider',
    imports: [
        MatDialogModule,
        MatTabsModule,
        MatButtonModule,
        NifiSpinnerDirective,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatInputModule,
        ParameterProviderReferences,
        PropertyTable,
        CommonModule,
        NifiTooltipDirective,
        PropertyVerification,
        ContextErrorBanner,
        CopyDirective
    ],
    templateUrl: './edit-parameter-provider.component.html',
    styleUrls: ['./edit-parameter-provider.component.scss']
})
export class EditParameterProvider extends TabbedDialog {
    @Input() createNewProperty!: (existingProperties: string[], allowsSensitive: boolean) => Observable<Property>;
    @Input() createNewService!: (request: InlineServiceCreationRequest) => Observable<InlineServiceCreationResponse>;
    @Input() goToService!: (serviceId: string) => void;
    @Input() goToReferencingParameterContext!: (parameterContextId: string) => void;
    @Input() saving$!: Observable<boolean>;
    @Input() propertyVerificationResults$!: Observable<ConfigVerificationResult[]>;
    @Input() propertyVerificationStatus$: Observable<'pending' | 'loading' | 'success'> = of('pending');

    @Output() verify: EventEmitter<VerifyPropertiesRequestContext> = new EventEmitter<VerifyPropertiesRequestContext>();
    @Output() editParameterProvider: EventEmitter<UpdateParameterProviderRequest> =
        new EventEmitter<UpdateParameterProviderRequest>();

    editParameterProviderForm: FormGroup;
    readonly: boolean;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditParameterProviderRequest,
        private formBuilder: FormBuilder,
        private client: Client,
        private nifiCommon: NiFiCommon,
        private clusterConnectionService: ClusterConnectionService
    ) {
        super('edit-parameter-provider-selected-index');

        this.readonly = !request.parameterProvider.permissions.canWrite;

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
            properties: new FormControl({ value: properties, disabled: this.readonly }),
            comments: new FormControl(request.parameterProvider.component.comments)
        });
    }

    formatType(entity: ParameterProviderEntity): string {
        return this.nifiCommon.formatType(entity.component);
    }

    formatBundle(entity: ParameterProviderEntity): string {
        return this.nifiCommon.formatBundle(entity.component.bundle);
    }

    submitForm(postUpdateNavigation?: string[], postUpdateNavigationBoundary?: string[]) {
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
            payload.component.properties = this.getModifiedProperties();
            payload.component.sensitiveDynamicPropertyNames = properties
                .filter((property) => property.descriptor.dynamic && property.descriptor.sensitive)
                .map((property) => property.descriptor.name);
        }

        this.editParameterProvider.next({
            payload,
            postUpdateNavigation,
            postUpdateNavigationBoundary
        });
    }

    navigateToParameterContext(parameterContextReference: ParameterContextReferenceEntity) {
        if (parameterContextReference.component?.id) {
            this.goToReferencingParameterContext(parameterContextReference.component?.id);
        }
    }

    protected readonly TextTip = TextTip;

    private getModifiedProperties(): ModifiedProperties {
        const propertyControl: AbstractControl | null = this.editParameterProviderForm.get('properties');
        if (propertyControl && propertyControl.dirty) {
            const properties: Property[] = propertyControl.value;
            const values: { [key: string]: string | null } = {};
            properties.forEach((property) => (values[property.property] = property.value));
            return values;
        }
        return {};
    }

    override isDirty(): boolean {
        return this.editParameterProviderForm.dirty;
    }

    verifyClicked(entity: ParameterProviderEntity): void {
        this.verify.next({
            entity,
            properties: this.getModifiedProperties()
        });
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
