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
import { Client } from '../../../../service/client.service';
import {
    ControllerServiceEntity,
    ControllerServiceReferencingComponent,
    EditControllerServiceDialogRequest,
    InlineServiceCreationRequest,
    InlineServiceCreationResponse,
    ParameterContextEntity,
    Property,
    UpdateControllerServiceRequest
} from '../../../../state/shared';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { NiFiCommon, TextTip, NifiTooltipDirective, CopyDirective } from '@nifi/shared';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { PropertyTable } from '../../property-table/property-table.component';
import { ControllerServiceApi } from '../controller-service-api/controller-service-api.component';
import { Observable, of } from 'rxjs';
import { ControllerServiceReferences } from '../controller-service-references/controller-service-references.component';
import { NifiSpinnerDirective } from '../../spinner/nifi-spinner.directive';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { ConvertToParameterResponse } from '../../../../pages/flow-designer/service/parameter-helper.service';
import { PropertyVerification } from '../../property-verification/property-verification.component';
import {
    ConfigVerificationResult,
    ModifiedProperties,
    VerifyPropertiesRequestContext
} from '../../../../state/property-verification';
import { TabbedDialog } from '../../tabbed-dialog/tabbed-dialog.component';
import { ErrorContextKey } from '../../../../state/error';
import { ContextErrorBanner } from '../../context-error-banner/context-error-banner.component';

@Component({
    selector: 'edit-controller-service',
    templateUrl: './edit-controller-service.component.html',
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        MatTabsModule,
        MatOptionModule,
        MatSelectModule,
        PropertyTable,
        ControllerServiceApi,
        ControllerServiceReferences,
        AsyncPipe,
        NifiSpinnerDirective,
        NifiTooltipDirective,
        PropertyVerification,
        ContextErrorBanner,
        CopyDirective
    ],
    styleUrls: ['./edit-controller-service.component.scss']
})
export class EditControllerService extends TabbedDialog {
    @Input() createNewProperty!: (existingProperties: string[], allowsSensitive: boolean) => Observable<Property>;
    @Input() createNewService!: (request: InlineServiceCreationRequest) => Observable<InlineServiceCreationResponse>;
    @Input() parameterContext: ParameterContextEntity | undefined;
    @Input() goToParameter!: (parameter: string) => void;
    @Input() convertToParameter!: (
        name: string,
        sensitive: boolean,
        value: string | null
    ) => Observable<ConvertToParameterResponse>;
    @Input() goToService!: (serviceId: string) => void;
    @Input() goToReferencingComponent!: (component: ControllerServiceReferencingComponent) => void;
    @Input() saving$!: Observable<boolean>;
    @Input() supportsParameters: boolean = true;
    @Input() propertyVerificationResults$!: Observable<ConfigVerificationResult[]>;
    @Input() propertyVerificationStatus$: Observable<'pending' | 'loading' | 'success'> = of('pending');

    @Output() verify: EventEmitter<VerifyPropertiesRequestContext> = new EventEmitter<VerifyPropertiesRequestContext>();
    @Output() editControllerService: EventEmitter<UpdateControllerServiceRequest> =
        new EventEmitter<UpdateControllerServiceRequest>();

    editControllerServiceForm: FormGroup;
    readonly: boolean;

    bulletinLevels = [
        {
            text: 'DEBUG',
            value: 'DEBUG'
        },
        {
            text: 'INFO',
            value: 'INFO'
        },
        {
            text: 'WARN',
            value: 'WARN'
        },
        {
            text: 'ERROR',
            value: 'ERROR'
        },
        {
            text: 'NONE',
            value: 'NONE'
        }
    ];

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditControllerServiceDialogRequest,
        private formBuilder: FormBuilder,
        private client: Client,
        private nifiCommon: NiFiCommon,
        private clusterConnectionService: ClusterConnectionService
    ) {
        super('edit-controller-service-selected-index');

        this.readonly =
            !request.controllerService.permissions.canWrite ||
            request.controllerService.status.runStatus !== 'DISABLED';

        const serviceProperties: any = request.controllerService.component.properties;
        const properties: Property[] = Object.entries(serviceProperties).map((entry: any) => {
            const [property, value] = entry;
            return {
                property,
                value,
                descriptor: request.controllerService.component.descriptors[property]
            };
        });

        // build the form
        this.editControllerServiceForm = this.formBuilder.group({
            name: new FormControl(request.controllerService.component.name, Validators.required),
            bulletinLevel: new FormControl(request.controllerService.component.bulletinLevel, Validators.required),
            properties: new FormControl({ value: properties, disabled: this.readonly }),
            comments: new FormControl(request.controllerService.component.comments)
        });
    }

    formatType(entity: ControllerServiceEntity): string {
        return this.nifiCommon.formatType(entity.component);
    }

    formatBundle(entity: ControllerServiceEntity): string {
        return this.nifiCommon.formatBundle(entity.component.bundle);
    }

    submitForm(postUpdateNavigation?: string[], postUpdateNavigationBoundary?: string[]) {
        const payload: any = {
            revision: this.client.getRevision(this.request.controllerService),
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                id: this.request.controllerService.id,
                name: this.editControllerServiceForm.get('name')?.value,
                comments: this.editControllerServiceForm.get('comments')?.value
            }
        };

        const propertyControl: AbstractControl | null = this.editControllerServiceForm.get('properties');
        if (propertyControl && propertyControl.dirty) {
            const properties: Property[] = propertyControl.value;
            payload.component.properties = this.getModifiedProperties();
            payload.component.sensitiveDynamicPropertyNames = properties
                .filter((property) => property.descriptor.dynamic && property.descriptor.sensitive)
                .map((property) => property.descriptor.name);
        }

        this.editControllerService.next({
            payload,
            postUpdateNavigation,
            postUpdateNavigationBoundary
        });
    }

    private getModifiedProperties(): ModifiedProperties {
        const propertyControl: AbstractControl | null = this.editControllerServiceForm.get('properties');
        if (propertyControl && propertyControl.dirty) {
            const properties: Property[] = propertyControl.value;
            const values: { [key: string]: string | null } = {};
            properties.forEach((property) => (values[property.property] = property.value));
            return values;
        }
        return {};
    }

    protected readonly TextTip = TextTip;

    override isDirty(): boolean {
        return this.editControllerServiceForm.dirty;
    }

    verifyClicked(entity: ControllerServiceEntity): void {
        this.verify.next({
            entity,
            properties: this.getModifiedProperties()
        });
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
