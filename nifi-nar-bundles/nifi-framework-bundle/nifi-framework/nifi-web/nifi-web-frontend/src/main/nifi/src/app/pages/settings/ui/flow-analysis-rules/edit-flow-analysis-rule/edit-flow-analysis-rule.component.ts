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
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { AbstractControl, FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { Observable } from 'rxjs';
import { Client } from '../../../../../service/client.service';
import {
    InlineServiceCreationRequest,
    InlineServiceCreationResponse,
    Property,
    SelectOption,
    TextTipInput
} from '../../../../../state/shared';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { PropertyTable } from '../../../../../ui/common/property-table/property-table.component';
import { NifiSpinnerDirective } from '../../../../../ui/common/spinner/nifi-spinner.directive';
import { NifiTooltipDirective } from '../../../../../ui/common/tooltips/nifi-tooltip.directive';
import { TextTip } from '../../../../../ui/common/tooltips/text-tip/text-tip.component';
import {
    EditFlowAnalysisRuleDialogRequest,
    FlowAnalysisRuleEntity,
    UpdateFlowAnalysisRuleRequest
} from '../../../state/flow-analysis-rules';
import { FlowAnalysisRuleTable } from '../flow-analysis-rule-table/flow-analysis-rule-table.component';
import { ErrorBanner } from '../../../../../ui/common/error-banner/error-banner.component';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';

@Component({
    selector: 'edit-flow-analysis-rule',
    standalone: true,
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
        MatTooltipModule,
        NifiTooltipDirective,
        FlowAnalysisRuleTable,
        ErrorBanner
    ],
    styleUrls: ['./edit-flow-analysis-rule.component.scss']
})
export class EditFlowAnalysisRule {
    @Input() createNewProperty!: (existingProperties: string[], allowsSensitive: boolean) => Observable<Property>;
    @Input() createNewService!: (request: InlineServiceCreationRequest) => Observable<InlineServiceCreationResponse>;
    @Input() saving$!: Observable<boolean>;
    @Input() goToService!: (serviceId: string) => void;
    @Output() editFlowAnalysisRule: EventEmitter<UpdateFlowAnalysisRuleRequest> =
        new EventEmitter<UpdateFlowAnalysisRuleRequest>();

    editFlowAnalysisRuleForm: FormGroup;

    strategies: SelectOption[] = [
        {
            text: 'Enforce',
            value: 'ENFORCE',
            description: 'Treat violations of this rule as errors the correction of which is mandatory.'
        }
    ];

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditFlowAnalysisRuleDialogRequest,
        private formBuilder: FormBuilder,
        private client: Client,
        private nifiCommon: NiFiCommon,
        private clusterConnectionService: ClusterConnectionService
    ) {
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
            enforcementPolicy: new FormControl('ENFORCE', Validators.required),
            properties: new FormControl(properties),
            comments: new FormControl(request.flowAnalysisRule.component.comments)
        });
    }

    formatType(entity: FlowAnalysisRuleEntity): string {
        return this.nifiCommon.formatType(entity.component);
    }

    formatBundle(entity: FlowAnalysisRuleEntity): string {
        return this.nifiCommon.formatBundle(entity.component.bundle);
    }

    submitForm(postUpdateNavigation?: string[]) {
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
            const values: { [key: string]: string | null } = {};
            properties.forEach((property) => (values[property.property] = property.value));
            payload.component.properties = values;
            payload.component.sensitiveDynamicPropertyNames = properties
                .filter((property) => property.descriptor.dynamic && property.descriptor.sensitive)
                .map((property) => property.descriptor.name);
        }

        this.editFlowAnalysisRule.next({
            payload,
            postUpdateNavigation
        });
    }

    getPropertyTipData(option: SelectOption): TextTipInput {
        return {
            // @ts-ignore
            text: option.description
        };
    }

    protected readonly TextTip = TextTip;
}
