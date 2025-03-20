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
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { AbstractControl, FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { Observable, of } from 'rxjs';
import { Client } from '../../../../../service/client.service';
import { InlineServiceCreationRequest, InlineServiceCreationResponse, Property } from '../../../../../state/shared';
import { CopyDirective, NiFiCommon, NifiTooltipDirective, TextTip, SelectOption } from '@nifi/shared';
import { PropertyTable } from '../../../../../ui/common/property-table/property-table.component';
import { NifiSpinnerDirective } from '../../../../../ui/common/spinner/nifi-spinner.directive';
import {
    EditFlowAnalysisRuleDialogRequest,
    FlowAnalysisRuleEntity,
    UpdateFlowAnalysisRuleRequest
} from '../../../state/flow-analysis-rules';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';
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
    selector: 'edit-flow-analysis-rule',
    templateUrl: './edit-flow-analysis-rule.component.html',
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatButtonModule,
        MatTabsModule,
        MatOptionModule,
        MatSelectModule,
        PropertyTable,
        AsyncPipe,
        NifiSpinnerDirective,
        NifiTooltipDirective,
        PropertyVerification,
        ContextErrorBanner,
        CopyDirective
    ],
    styleUrls: ['./edit-flow-analysis-rule.component.scss']
})
export class EditFlowAnalysisRule extends TabbedDialog {
    @Input() createNewProperty!: (existingProperties: string[], allowsSensitive: boolean) => Observable<Property>;
    @Input() createNewService!: (request: InlineServiceCreationRequest) => Observable<InlineServiceCreationResponse>;
    @Input() saving$!: Observable<boolean>;
    @Input() goToService!: (serviceId: string) => void;
    @Input() propertyVerificationResults$!: Observable<ConfigVerificationResult[]>;
    @Input() propertyVerificationStatus$: Observable<'pending' | 'loading' | 'success'> = of('pending');

    @Output() verify: EventEmitter<VerifyPropertiesRequestContext> = new EventEmitter<VerifyPropertiesRequestContext>();
    @Output() editFlowAnalysisRule: EventEmitter<UpdateFlowAnalysisRuleRequest> =
        new EventEmitter<UpdateFlowAnalysisRuleRequest>();

    editFlowAnalysisRuleForm: FormGroup;
    readonly: boolean;

    strategies: SelectOption[] = [
        {
            text: 'Enforce',
            value: 'ENFORCE',
            description: 'Treat violations of this rule as errors the correction of which is mandatory.'
        },
        {
            text: 'Warn',
            value: 'WARN',
            description: 'Treat violations of this rule as warnings the correction of which is optional.'
        }
    ];

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditFlowAnalysisRuleDialogRequest,
        private formBuilder: FormBuilder,
        private client: Client,
        private nifiCommon: NiFiCommon,
        private clusterConnectionService: ClusterConnectionService
    ) {
        super('edit-flow-analysis-rule-selected-index');

        this.readonly =
            !request.flowAnalysisRule.permissions.canWrite || request.flowAnalysisRule.status.runStatus !== 'DISABLED';

        const serviceProperties: any = request.flowAnalysisRule.component.properties;
        const properties: Property[] = Object.entries(serviceProperties).map((entry: any) => {
            const [property, value] = entry;
            return {
                property,
                value,
                descriptor: request.flowAnalysisRule.component.descriptors[property]
            };
        });

        // build the form
        this.editFlowAnalysisRuleForm = this.formBuilder.group({
            name: new FormControl(request.flowAnalysisRule.component.name, Validators.required),
            state: new FormControl(request.flowAnalysisRule.component.state === 'STOPPED', Validators.required),
            enforcementPolicy: new FormControl(
                request.flowAnalysisRule.component.enforcementPolicy,
                Validators.required
            ),
            properties: new FormControl({ value: properties, disabled: this.readonly }),
            comments: new FormControl(request.flowAnalysisRule.component.comments)
        });
    }

    formatType(entity: FlowAnalysisRuleEntity): string {
        return this.nifiCommon.formatType(entity.component);
    }

    formatBundle(entity: FlowAnalysisRuleEntity): string {
        return this.nifiCommon.formatBundle(entity.component.bundle);
    }

    submitForm(postUpdateNavigation?: string[], postUpdateNavigationBoundary?: string[]) {
        const payload: any = {
            revision: this.client.getRevision(this.request.flowAnalysisRule),
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                id: this.request.flowAnalysisRule.id,
                name: this.editFlowAnalysisRuleForm.get('name')?.value,
                comments: this.editFlowAnalysisRuleForm.get('comments')?.value,
                enforcementPolicy: this.editFlowAnalysisRuleForm.get('enforcementPolicy')?.value,
                state: this.editFlowAnalysisRuleForm.get('state')?.value ? 'STOPPED' : 'DISABLED'
            }
        };

        const propertyControl: AbstractControl | null = this.editFlowAnalysisRuleForm.get('properties');
        if (propertyControl && propertyControl.dirty) {
            const properties: Property[] = propertyControl.value;
            payload.component.properties = this.getModifiedProperties();
            payload.component.sensitiveDynamicPropertyNames = properties
                .filter((property) => property.descriptor.dynamic && property.descriptor.sensitive)
                .map((property) => property.descriptor.name);
        }

        this.editFlowAnalysisRule.next({
            payload,
            postUpdateNavigation,
            postUpdateNavigationBoundary
        });
    }

    protected readonly TextTip = TextTip;

    private getModifiedProperties(): ModifiedProperties {
        const propertyControl: AbstractControl | null = this.editFlowAnalysisRuleForm.get('properties');
        if (propertyControl && propertyControl.dirty) {
            const properties: Property[] = propertyControl.value;
            const values: { [key: string]: string | null } = {};
            properties.forEach((property) => (values[property.property] = property.value));
            return values;
        }
        return {};
    }

    override isDirty(): boolean {
        return this.editFlowAnalysisRuleForm.dirty;
    }

    verifyClicked(entity: FlowAnalysisRuleEntity): void {
        this.verify.next({
            entity,
            properties: this.getModifiedProperties()
        });
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
