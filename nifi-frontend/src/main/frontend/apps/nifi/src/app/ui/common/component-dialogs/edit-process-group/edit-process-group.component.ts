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

import { Component, EventEmitter, Input, Output, inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import {
    AbstractControl,
    FormBuilder,
    FormControl,
    FormGroup,
    FormsModule,
    ReactiveFormsModule,
    ValidationErrors,
    Validators
} from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { Observable } from 'rxjs';
import { Client } from '../../../../service/client.service';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { TabbedDialog, TABBED_DIALOG_ID } from '../../tabbed-dialog/tabbed-dialog.component';
import { ErrorContextKey } from '../../../../state/error';
import { ContextErrorBanner } from '../../context-error-banner/context-error-banner.component';
import { EditComponentDialogRequest, ParameterContextEntity } from '../../../../state/shared';
import { CurrentUser } from '../../../../state/current-user';
import {
    NifiSpinnerDirective,
    NifiTooltipDirective,
    SelectOption,
    SortObjectByPropertyPipe,
    TextTip
} from '@nifi/shared';

@Component({
    selector: 'edit-process-group',
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
        NifiSpinnerDirective,
        NifiTooltipDirective,
        FormsModule,
        ContextErrorBanner,
        SortObjectByPropertyPipe
    ],
    styleUrls: ['./edit-process-group.component.scss'],
    providers: [
        {
            provide: TABBED_DIALOG_ID,
            useValue: 'edit-process-group-selected-index'
        }
    ]
})
export class EditProcessGroup extends TabbedDialog {
    private static readonly DATA_SIZE_PATTERN = /^(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB)$/i;
    private static readonly MAXIMUM_HEAP_PERCENTAGE = 90;
    private static readonly DATA_SIZE_MULTIPLIERS = {
        B: 1n,
        KB: 1n << 10n,
        MB: 1n << 20n,
        GB: 1n << 30n,
        TB: 1n << 40n
    };
    private static readonly MAXIMUM_DATA_SIZE_BYTES = 9223372036854775807n;

    request = inject<EditComponentDialogRequest>(MAT_DIALOG_DATA);
    private formBuilder = inject(FormBuilder);
    private client = inject(Client);
    private clusterConnectionService = inject(ClusterConnectionService);

    @Input() set parameterContexts(parameterContexts: ParameterContextEntity[]) {
        if (parameterContexts !== undefined) {
            this.parameterContextsOptions = [];
            this._parameterContexts = parameterContexts;

            parameterContexts.forEach((parameterContext) => {
                if (parameterContext.permissions.canRead && parameterContext.component) {
                    this.parameterContextsOptions.push({
                        text: parameterContext.component.name,
                        value: parameterContext.id,
                        description: parameterContext.component.description
                    });
                } else {
                    let disabled: boolean;
                    if (this.request.entity.component.parameterContext) {
                        disabled = this.request.entity.component.parameterContext.id !== parameterContext.id;
                    } else {
                        disabled = true;
                    }

                    this.parameterContextsOptions.push({
                        text: parameterContext.id,
                        value: parameterContext.id,
                        disabled
                    });
                }
            });

            if (this.request.entity.component.parameterContext) {
                this.editProcessGroupForm
                    .get('parameterContext')
                    ?.setValue(this.request.entity.component.parameterContext.id);
            }
        }
    }

    get parameterContexts() {
        return this._parameterContexts;
    }

    @Input() saving$!: Observable<boolean>;
    @Input() currentUser$!: Observable<CurrentUser>;
    @Output() editProcessGroup: EventEmitter<any> = new EventEmitter<any>();
    @Output() openNewParameterContext: EventEmitter<ParameterContextEntity[]> = new EventEmitter<
        ParameterContextEntity[]
    >();

    protected readonly TextTip = TextTip;
    protected readonly STATELESS: string = 'STATELESS';
    private initialMaxConcurrentTasks: number;
    private initialStatelessFlowTimeout: string;
    private initialStatelessFlowFileContentInMemoryMax: string;
    private initialStatelessFlowFileContentInMemoryHeapPercentage: string | number;
    private _parameterContexts: ParameterContextEntity[] = [];

    editProcessGroupForm: FormGroup;
    readonly: boolean;
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
            value: this.STATELESS,
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

    constructor() {
        super();
        const request = this.request;

        this.readonly = !request.entity.permissions.canWrite;

        this.editProcessGroupForm = this.formBuilder.group({
            name: new FormControl(request.entity.component.name, Validators.required),
            applyParameterContextRecursively: new FormControl({ value: false, disabled: this.readonly }),
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
            comments: new FormControl(request.entity.component.comments),
            parameterContext: new FormControl(null)
        });

        this.initialMaxConcurrentTasks = request.entity.component.maxConcurrentTasks;
        this.initialStatelessFlowTimeout = request.entity.component.statelessFlowTimeout;
        this.initialStatelessFlowFileContentInMemoryMax =
            request.entity.component.statelessFlowFileContentInMemoryMax ?? '';
        this.initialStatelessFlowFileContentInMemoryHeapPercentage =
            request.entity.component.statelessFlowFileContentInMemoryHeapPercentage ?? 0;

        this.executionEngineChanged(request.entity.component.executionEngine);
    }

    executionEngineChanged(value: string): void {
        if (value == this.STATELESS) {
            this.editProcessGroupForm.addControl(
                'maxConcurrentTasks',
                new FormControl(this.initialMaxConcurrentTasks, Validators.required)
            );
            this.editProcessGroupForm.addControl(
                'statelessFlowTimeout',
                new FormControl(this.initialStatelessFlowTimeout, Validators.required)
            );
            this.editProcessGroupForm.addControl(
                'statelessFlowFileContentInMemoryMax',
                new FormControl(
                    {
                        value: this.initialStatelessFlowFileContentInMemoryMax,
                        disabled: this.request.entity.component.statelessGroupScheduledState !== 'STOPPED'
                    },
                    EditProcessGroup.validateInMemoryContentMax
                )
            );
            this.editProcessGroupForm.addControl(
                'statelessFlowFileContentInMemoryHeapPercentage',
                new FormControl(
                    {
                        value: this.initialStatelessFlowFileContentInMemoryHeapPercentage,
                        disabled: this.request.entity.component.statelessGroupScheduledState !== 'STOPPED'
                    },
                    EditProcessGroup.validateInMemoryHeapPercentage
                )
            );
        } else {
            this.editProcessGroupForm.removeControl('maxConcurrentTasks');
            this.editProcessGroupForm.removeControl('statelessFlowTimeout');
            this.editProcessGroupForm.removeControl('statelessFlowFileContentInMemoryMax');
            this.editProcessGroupForm.removeControl('statelessFlowFileContentInMemoryHeapPercentage');
        }
    }

    private static validateInMemoryContentMax(control: AbstractControl): ValidationErrors | null {
        if (control.value === null || control.value === undefined || control.value === '') {
            return null;
        }

        if (typeof control.value !== 'string') {
            return { dataSize: true };
        }

        const trimmedValue = control.value.trim();
        if (trimmedValue.length === 0) {
            return null;
        }

        const match = EditProcessGroup.DATA_SIZE_PATTERN.exec(trimmedValue);
        if (match === null) {
            return { dataSize: true };
        }

        const size = match[1];
        const unit = match[2].toUpperCase() as keyof typeof EditProcessGroup.DATA_SIZE_MULTIPLIERS;
        const [integerPart, fractionalPart = ''] = size.split('.');
        const unscaledSize = BigInt(integerPart + fractionalPart);
        const divisor = 10n ** BigInt(fractionalPart.length);
        const unscaledBytes = unscaledSize * EditProcessGroup.DATA_SIZE_MULTIPLIERS[unit];

        if (unscaledBytes % divisor !== 0n || unscaledBytes / divisor > EditProcessGroup.MAXIMUM_DATA_SIZE_BYTES) {
            return { dataSize: true };
        }

        return null;
    }

    private static validateInMemoryHeapPercentage(control: AbstractControl): ValidationErrors | null {
        if (control.value === null || control.value === undefined || control.value === '') {
            return null;
        }

        const percentage = Number(control.value);
        if (!Number.isInteger(percentage) || percentage < 0 || percentage > EditProcessGroup.MAXIMUM_HEAP_PERCENTAGE) {
            return { heapPercentage: true };
        }

        return null;
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

        if (this.editProcessGroupForm.get('executionEngine')?.value === this.STATELESS) {
            payload.component.maxConcurrentTasks = this.editProcessGroupForm.get('maxConcurrentTasks')?.value;
            payload.component.statelessFlowTimeout = this.editProcessGroupForm.get('statelessFlowTimeout')?.value;
            payload.component.statelessFlowFileContentInMemoryMax =
                this.editProcessGroupForm.get('statelessFlowFileContentInMemoryMax')?.value ?? '';
            const heapPercentage = this.editProcessGroupForm.get(
                'statelessFlowFileContentInMemoryHeapPercentage'
            )?.value;
            payload.component.statelessFlowFileContentInMemoryHeapPercentage =
                heapPercentage === null || heapPercentage === undefined || heapPercentage === ''
                    ? ''
                    : String(heapPercentage);
        }

        this.editProcessGroup.next(payload);
    }

    openNewParameterContextDialog(): void {
        this.openNewParameterContext.emit(this._parameterContexts);
    }

    override isDirty(): boolean {
        return this.editProcessGroupForm.dirty;
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
