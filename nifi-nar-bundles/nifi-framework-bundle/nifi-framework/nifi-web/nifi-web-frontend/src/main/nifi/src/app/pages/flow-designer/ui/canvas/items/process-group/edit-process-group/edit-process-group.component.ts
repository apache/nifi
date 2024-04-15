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
import { FormBuilder, FormControl, FormGroup, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { Observable } from 'rxjs';
import { SelectOption, TextTipInput } from '../../../../../../../state/shared';
import { Client } from '../../../../../../../service/client.service';
import { PropertyTable } from '../../../../../../../ui/common/property-table/property-table.component';
import { NifiSpinnerDirective } from '../../../../../../../ui/common/spinner/nifi-spinner.directive';
import { NifiTooltipDirective } from '../../../../../../../ui/common/tooltips/nifi-tooltip.directive';
import { TextTip } from '../../../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { ParameterContextEntity } from '../../../../../../parameter-contexts/state/parameter-context-listing';
import { ControllerServiceTable } from '../../../../../../../ui/common/controller-service/controller-service-table/controller-service-table.component';
import { EditComponentDialogRequest } from '../../../../../state/flow';
import { ClusterConnectionService } from '../../../../../../../service/cluster-connection.service';

@Component({
    selector: 'edit-process-group',
    standalone: true,
    templateUrl: './edit-process-group.component.html',
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        MatTabsModule,
        MatOptionModule,
        MatSelectModule,
        AsyncPipe,
        PropertyTable,
        NifiSpinnerDirective,
        NifiTooltipDirective,
        FormsModule,
        ControllerServiceTable
    ],
    styleUrls: ['./edit-process-group.component.scss']
})
export class EditProcessGroup {
    @Input() set parameterContexts(parameterContexts: ParameterContextEntity[]) {
        parameterContexts.forEach((parameterContext) => {
            if (parameterContext.permissions.canRead) {
                this.parameterContextsOptions.push({
                    text: parameterContext.component.name,
                    value: parameterContext.id,
                    description: parameterContext.component.description
                });
            }
        });

        if (this.request.entity.component.parameterContext) {
            this.editProcessGroupForm.addControl(
                'parameterContext',
                new FormControl(this.request.entity.component.parameterContext.id)
            );
        } else {
            this.editProcessGroupForm.addControl('parameterContext', new FormControl(null));
        }
    }
    @Input() saving$!: Observable<boolean>;
    @Output() editProcessGroup: EventEmitter<any> = new EventEmitter<any>();

    protected readonly TextTip = TextTip;

    editProcessGroupForm: FormGroup;
    parameterContextsOptions: SelectOption[] = [];

    executionEngineOptions: SelectOption[] = [
        {
            text: 'Inherited',
            value: 'INHERITED',
            description: 'Use whichever Execution Engine the parent Process Group is configured to use.'
        },
        {
            text: 'Standard',
            value: 'STANDARD',
            description: 'Use the Standard NiFi Execution Engine. See the User Guide for additional details.'
        },
        {
            text: 'Stateless',
            value: 'STATELESS',
            description:
                'Run the dataflow using the Stateless Execution Engine. See the User Guide for additional details.'
        }
    ];

    flowfileConcurrencyOptions: SelectOption[] = [
        {
            text: 'Single FlowFile Per Node',
            value: 'SINGLE_FLOWFILE_PER_NODE',
            description:
                'Only a single FlowFile is to be allowed to enter the Process Group at a time on each node in the cluster. While that FlowFile may be split into many or ' +
                'spawn many children, no additional FlowFiles will be allowed to enter the Process Group through a Local Input Port until the previous FlowFile ' +
                '- and all of its child/descendent FlowFiles - have been processed.'
        },
        {
            text: 'Single Batch Per Node',
            value: 'SINGLE_BATCH_PER_NODE',
            description:
                'When an Input Port pulls a FlowFile into the Process Group, FlowFiles will continue to be ingested into the Process Group until all input queues ' +
                'have been emptied. At that point, no additional FlowFiles will be allowed to enter the Process Group through a Local Input Port until the entire batch ' +
                'of FlowFiles has been processed.'
        },
        {
            text: 'Unbounded',
            value: 'UNBOUNDED',
            description: 'The number of FlowFiles that can be processed concurrently is unbounded.'
        }
    ];

    flowfileOutboundPolicyOptions: SelectOption[] = [
        {
            text: 'Stream When Available',
            value: 'STREAM_WHEN_AVAILABLE',
            description:
                'FlowFiles that are queued up to be transferred out of a Process Group by an Output Port will be transferred out ' +
                'of the Process Group as soon as they are available.'
        },
        {
            text: 'Batch Output',
            value: 'BATCH_OUTPUT',
            description:
                'FlowFiles that are queued up to be transferred out of a Process Group by an Output Port will remain queued until ' +
                'all FlowFiles in the Process Group are ready to be transferred out of the group. The FlowFiles will then be transferred ' +
                'out of the group. This setting will be ignored if the FlowFile Concurrency is Unbounded.'
        }
    ];

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditComponentDialogRequest,
        private formBuilder: FormBuilder,
        private client: Client,
        private clusterConnectionService: ClusterConnectionService
    ) {
        this.parameterContextsOptions.push({
            text: 'No parameter context',
            value: null
        });

        this.editProcessGroupForm = this.formBuilder.group({
            name: new FormControl(request.entity.component.name, Validators.required),
            applyParameterContextRecursively: new FormControl(false),
            executionEngine: new FormControl(request.entity.component.executionEngine, Validators.required),
            flowfileConcurrency: new FormControl(request.entity.component.flowfileConcurrency, Validators.required),
            flowfileOutboundPolicy: new FormControl(
                request.entity.component.flowfileOutboundPolicy,
                Validators.required
            ),
            defaultFlowFileExpiration: new FormControl(
                request.entity.component.defaultFlowFileExpiration,
                Validators.required
            ),
            defaultBackPressureObjectThreshold: new FormControl(
                request.entity.component.defaultBackPressureObjectThreshold,
                Validators.required
            ),
            defaultBackPressureDataSizeThreshold: new FormControl(
                request.entity.component.defaultBackPressureDataSizeThreshold,
                Validators.required
            ),
            logFileSuffix: new FormControl(request.entity.component.logFileSuffix),
            comments: new FormControl(request.entity.component.comments)
        });
    }

    getSelectOptionTipData(option: SelectOption): TextTipInput {
        return {
            // @ts-ignore
            text: option.description
        };
    }

    submitForm() {
        let updateStrategy = 'DIRECT_CHILDREN';
        if (this.editProcessGroupForm.get('applyParameterContextRecursively')?.value) {
            updateStrategy = 'ALL_DESCENDANTS';
        }

        const payload: any = {
            revision: this.client.getRevision(this.request.entity),
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            processGroupUpdateStrategy: updateStrategy,
            component: {
                id: this.request.entity.id,
                name: this.editProcessGroupForm.get('name')?.value,
                executionEngine: this.editProcessGroupForm.get('executionEngine')?.value,
                flowfileConcurrency: this.editProcessGroupForm.get('flowfileConcurrency')?.value,
                flowfileOutboundPolicy: this.editProcessGroupForm.get('flowfileOutboundPolicy')?.value,
                defaultFlowFileExpiration: this.editProcessGroupForm.get('defaultFlowFileExpiration')?.value,
                defaultBackPressureObjectThreshold: this.editProcessGroupForm.get('defaultBackPressureObjectThreshold')
                    ?.value,
                defaultBackPressureDataSizeThreshold: this.editProcessGroupForm.get(
                    'defaultBackPressureDataSizeThreshold'
                )?.value,
                logFileSuffix: this.editProcessGroupForm.get('logFileSuffix')?.value,
                parameterContext: {
                    id: this.editProcessGroupForm.get('parameterContext')?.value
                },
                comments: this.editProcessGroupForm.get('comments')?.value
            }
        };

        this.editProcessGroup.next(payload);
    }
}
