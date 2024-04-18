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
import { MatTabsModule } from '@angular/material/tabs';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { Observable } from 'rxjs';
import {
    InlineServiceCreationRequest,
    InlineServiceCreationResponse,
    Parameter,
    ParameterContextReferenceEntity,
    Property,
    SelectOption,
    TextTipInput
} from '../../../../../../../state/shared';
import { Client } from '../../../../../../../service/client.service';
import { NiFiCommon } from '../../../../../../../service/nifi-common.service';
import { EditComponentDialogRequest, UpdateProcessorRequest } from '../../../../../state/flow';
import { PropertyTable } from '../../../../../../../ui/common/property-table/property-table.component';
import { NifiSpinnerDirective } from '../../../../../../../ui/common/spinner/nifi-spinner.directive';
import { NifiTooltipDirective } from '../../../../../../../ui/common/tooltips/nifi-tooltip.directive';
import { TextTip } from '../../../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { RunDurationSlider } from './run-duration-slider/run-duration-slider.component';
import {
    RelationshipConfiguration,
    RelationshipSettings
} from './relationship-settings/relationship-settings.component';
import { ErrorBanner } from '../../../../../../../ui/common/error-banner/error-banner.component';
import { ClusterConnectionService } from '../../../../../../../service/cluster-connection.service';

@Component({
    selector: 'edit-processor',
    standalone: true,
    templateUrl: './edit-processor.component.html',
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
        RunDurationSlider,
        RelationshipSettings,
        ErrorBanner
    ],
    styleUrls: ['./edit-processor.component.scss']
})
export class EditProcessor {
    @Input() createNewProperty!: (existingProperties: string[], allowsSensitive: boolean) => Observable<Property>;
    @Input() createNewService!: (request: InlineServiceCreationRequest) => Observable<InlineServiceCreationResponse>;
    @Input() getParameters!: (sensitive: boolean) => Observable<Parameter[]>;
    @Input() parameterContext: ParameterContextReferenceEntity | undefined;
    @Input() goToParameter!: (parameter: string) => void;
    @Input() convertToParameter!: (name: string, sensitive: boolean, value: string | null) => Observable<string>;
    @Input() goToService!: (serviceId: string) => void;
    @Input() saving$!: Observable<boolean>;
    @Output() editProcessor: EventEmitter<UpdateProcessorRequest> = new EventEmitter<UpdateProcessorRequest>();

    protected readonly TextTip = TextTip;

    editProcessorForm: FormGroup;

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

    schedulingStrategies: SelectOption[] = [
        {
            text: 'Timer driven',
            value: 'TIMER_DRIVEN',
            description: 'Processor will be scheduled to run on an interval defined by the run schedule.'
        },
        {
            text: 'CRON driven',
            value: 'CRON_DRIVEN',
            description: 'Processor will be scheduled to run on at specific times based on the specified CRON string.'
        }
    ];

    executionStrategies: SelectOption[] = [
        {
            text: 'All nodes',
            value: 'ALL',
            description: 'Processor will be scheduled to run on all nodes'
        },
        {
            text: 'Primary node',
            value: 'PRIMARY',
            description: 'Processor will be scheduled to run only on the primary node'
        }
    ];

    schedulingStrategy: string;
    cronDrivenConcurrentTasks: string;
    cronDrivenSchedulingPeriod: string;
    timerDrivenConcurrentTasks: string;
    timerDrivenSchedulingPeriod: string;
    runDurationMillis: number;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditComponentDialogRequest,
        private formBuilder: FormBuilder,
        private client: Client,
        private clusterConnectionService: ClusterConnectionService,
        private nifiCommon: NiFiCommon
    ) {
        const processorProperties: any = request.entity.component.config.properties;
        const properties: Property[] = Object.entries(processorProperties).map((entry: any) => {
            const [property, value] = entry;
            return {
                property,
                value,
                descriptor: request.entity.component.config.descriptors[property]
            };
        });

        const defaultConcurrentTasks: any = request.entity.component.config.defaultConcurrentTasks;
        const defaultSchedulingPeriod: any = request.entity.component.config.defaultSchedulingPeriod;

        let concurrentTasks: string;
        let schedulingPeriod: string;

        this.schedulingStrategy = request.entity.component.config.schedulingStrategy;
        if (this.schedulingStrategy === 'CRON_DRIVEN') {
            this.cronDrivenConcurrentTasks = request.entity.component.config.concurrentlySchedulableTaskCount;
            this.cronDrivenSchedulingPeriod = request.entity.component.config.schedulingPeriod;
            this.timerDrivenConcurrentTasks = defaultConcurrentTasks['TIMER_DRIVEN'];
            this.timerDrivenSchedulingPeriod = defaultSchedulingPeriod['TIMER_DRIVEN'];

            concurrentTasks = this.cronDrivenConcurrentTasks;
            schedulingPeriod = this.cronDrivenSchedulingPeriod;
        } else {
            this.cronDrivenConcurrentTasks = defaultConcurrentTasks['CRON_DRIVEN'];
            this.cronDrivenSchedulingPeriod = defaultSchedulingPeriod['CRON_DRIVEN'];
            this.timerDrivenConcurrentTasks = request.entity.component.config.concurrentlySchedulableTaskCount;
            this.timerDrivenSchedulingPeriod = request.entity.component.config.schedulingPeriod;

            concurrentTasks = this.timerDrivenConcurrentTasks;
            schedulingPeriod = this.timerDrivenSchedulingPeriod;
        }

        this.runDurationMillis = request.entity.component.config.runDurationMillis;

        const relationshipConfiguration: RelationshipConfiguration = {
            relationships: request.entity.component.relationships,
            backoffMechanism: request.entity.component.config.backoffMechanism,
            retryCount: request.entity.component.config.retryCount,
            maxBackoffPeriod: request.entity.component.config.maxBackoffPeriod
        };

        // build the form
        this.editProcessorForm = this.formBuilder.group({
            name: new FormControl(request.entity.component.name, Validators.required),
            penaltyDuration: new FormControl(request.entity.component.config.penaltyDuration, Validators.required),
            yieldDuration: new FormControl(request.entity.component.config.yieldDuration, Validators.required),
            bulletinLevel: new FormControl(request.entity.component.config.bulletinLevel, Validators.required),
            schedulingStrategy: new FormControl(this.schedulingStrategy, Validators.required),
            concurrentTasks: new FormControl(concurrentTasks, Validators.required),
            schedulingPeriod: new FormControl(schedulingPeriod, Validators.required),
            executionNode: new FormControl(request.entity.component.config.executionNode, Validators.required),
            properties: new FormControl(properties),
            relationshipConfiguration: new FormControl(relationshipConfiguration, Validators.required),
            comments: new FormControl(request.entity.component.config.comments)
        });

        if (this.supportsBatching()) {
            this.editProcessorForm.addControl(
                'runDuration',
                new FormControl(this.runDurationMillis, Validators.required)
            );
        }
    }

    supportsBatching(): boolean {
        return this.request.entity.component.supportsBatching == true;
    }

    formatType(entity: any): string {
        return this.nifiCommon.formatType(entity.component);
    }

    formatBundle(entity: any): string {
        return this.nifiCommon.formatBundle(entity.component.bundle);
    }

    getSelectOptionTipData(option: SelectOption): TextTipInput {
        return {
            // @ts-ignore
            text: option.description
        };
    }

    concurrentTasksChanged(): void {
        if (this.schedulingStrategy === 'CRON_DRIVEN') {
            this.cronDrivenConcurrentTasks = this.editProcessorForm.get('concurrentTasks')?.value;
        } else {
            this.timerDrivenConcurrentTasks = this.editProcessorForm.get('concurrentTasks')?.value;
        }
    }

    schedulingPeriodChanged(): void {
        if (this.schedulingStrategy === 'CRON_DRIVEN') {
            this.cronDrivenSchedulingPeriod = this.editProcessorForm.get('schedulingPeriod')?.value;
        } else {
            this.timerDrivenSchedulingPeriod = this.editProcessorForm.get('schedulingPeriod')?.value;
        }
    }

    schedulingStrategyChanged(value: string): void {
        this.schedulingStrategy = value;

        if (value === 'CRON_DRIVEN') {
            this.editProcessorForm.get('concurrentTasks')?.setValue(this.cronDrivenConcurrentTasks);
            this.editProcessorForm.get('schedulingPeriod')?.setValue(this.cronDrivenSchedulingPeriod);
        } else {
            this.editProcessorForm.get('concurrentTasks')?.setValue(this.timerDrivenConcurrentTasks);
            this.editProcessorForm.get('schedulingPeriod')?.setValue(this.timerDrivenSchedulingPeriod);
        }
    }

    executionStrategyDisabled(option: SelectOption): boolean {
        return option.value == 'ALL' && this.request.entity.component.executionNodeRestricted === true;
    }

    runDurationChanged(): void {
        this.runDurationMillis = this.editProcessorForm.get('runDuration')?.value;
    }

    shouldShowWarning(): boolean {
        return (
            this.runDurationMillis > 0 &&
            (this.request.entity.component.inputRequirement === 'INPUT_FORBIDDEN' ||
                this.request.entity.component.inputRequirement === 'INPUT_ALLOWED')
        );
    }

    submitForm(postUpdateNavigation?: string[]) {
        const relationshipConfiguration: RelationshipConfiguration =
            this.editProcessorForm.get('relationshipConfiguration')?.value;
        const autoTerminated: string[] = relationshipConfiguration.relationships
            .filter((relationship) => relationship.autoTerminate)
            .map((relationship) => relationship.name);
        const retried: string[] = relationshipConfiguration.relationships
            .filter((relationship) => relationship.retry)
            .map((relationship) => relationship.name);

        const payload: any = {
            revision: this.client.getRevision(this.request.entity),
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                id: this.request.entity.id,
                name: this.editProcessorForm.get('name')?.value,
                config: {
                    penaltyDuration: this.editProcessorForm.get('penaltyDuration')?.value,
                    yieldDuration: this.editProcessorForm.get('yieldDuration')?.value,
                    bulletinLevel: this.editProcessorForm.get('bulletinLevel')?.value,
                    schedulingStrategy: this.editProcessorForm.get('schedulingStrategy')?.value,
                    concurrentlySchedulableTaskCount: this.editProcessorForm.get('concurrentTasks')?.value,
                    schedulingPeriod: this.editProcessorForm.get('schedulingPeriod')?.value,
                    executionNode: this.editProcessorForm.get('executionNode')?.value,
                    autoTerminatedRelationships: autoTerminated,
                    retriedRelationships: retried,
                    comments: this.editProcessorForm.get('comments')?.value
                }
            }
        };

        const propertyControl: AbstractControl | null = this.editProcessorForm.get('properties');
        if (propertyControl && propertyControl.dirty) {
            const properties: Property[] = propertyControl.value;
            const values: { [key: string]: string | null } = {};
            properties.forEach((property) => (values[property.property] = property.value));
            payload.component.config.properties = values;
            payload.component.config.sensitiveDynamicPropertyNames = properties
                .filter((property) => property.descriptor.dynamic && property.descriptor.sensitive)
                .map((property) => property.descriptor.name);
        }

        if (this.supportsBatching()) {
            payload.component.config.runDurationMillis = this.editProcessorForm.get('runDuration')?.value;
        }

        if (retried.length > 0) {
            payload.component.config.backoffMechanism = relationshipConfiguration.backoffMechanism;
            payload.component.config.maxBackoffPeriod = relationshipConfiguration.maxBackoffPeriod;
            payload.component.config.retryCount = relationshipConfiguration.retryCount;
        }

        this.editProcessor.next({
            postUpdateNavigation,
            payload
        });
    }
}
