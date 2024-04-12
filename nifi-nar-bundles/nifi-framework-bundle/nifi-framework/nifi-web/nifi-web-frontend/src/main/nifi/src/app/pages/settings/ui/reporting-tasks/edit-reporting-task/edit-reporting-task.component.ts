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
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { AbstractControl, FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { Observable } from 'rxjs';
import { Client } from '../../../../../service/client.service';
import {
    ControllerServiceReferencingComponent,
    InlineServiceCreationRequest,
    InlineServiceCreationResponse,
    Property,
    SelectOption,
    TextTipInput
} from '../../../../../state/shared';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { PropertyTable } from '../../../../../ui/common/property-table/property-table.component';
import { NifiSpinnerDirective } from '../../../../../ui/common/spinner/nifi-spinner.directive';
import {
    EditReportingTaskDialogRequest,
    ReportingTaskEntity,
    UpdateReportingTaskRequest
} from '../../../state/reporting-tasks';
import { ControllerServiceApi } from '../../../../../ui/common/controller-service/controller-service-api/controller-service-api.component';
import { NifiTooltipDirective } from '../../../../../ui/common/tooltips/nifi-tooltip.directive';
import { TextTip } from '../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { ErrorBanner } from '../../../../../ui/common/error-banner/error-banner.component';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';

@Component({
    selector: 'edit-reporting-task',
    standalone: true,
    templateUrl: './edit-reporting-task.component.html',
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
        AsyncPipe,
        NifiSpinnerDirective,
        MatTooltipModule,
        NifiTooltipDirective,
        ErrorBanner
    ],
    styleUrls: ['./edit-reporting-task.component.scss']
})
export class EditReportingTask {
    @Input() createNewProperty!: (existingProperties: string[], allowsSensitive: boolean) => Observable<Property>;
    @Input() createNewService!: (request: InlineServiceCreationRequest) => Observable<InlineServiceCreationResponse>;
    @Input() goToService!: (serviceId: string) => void;
    @Input() goToReferencingComponent!: (component: ControllerServiceReferencingComponent) => void;
    @Input() saving$!: Observable<boolean>;
    @Output() editReportingTask: EventEmitter<UpdateReportingTaskRequest> =
        new EventEmitter<UpdateReportingTaskRequest>();

    editReportingTaskForm: FormGroup;
    schedulingStrategy: string;
    cronDrivenSchedulingPeriod: string;
    timerDrivenSchedulingPeriod: string;

    strategies: SelectOption[] = [
        {
            text: 'Timer driven',
            value: 'TIMER_DRIVEN',
            description: 'Reporting task will be scheduled on an interval defined by the run schedule.'
        },
        {
            text: 'CRON driven',
            value: 'CRON_DRIVEN',
            description:
                'Reporting task will be scheduled to run on at specific times based on the specified CRON string.'
        }
    ];

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditReportingTaskDialogRequest,
        private formBuilder: FormBuilder,
        private client: Client,
        private nifiCommon: NiFiCommon,
        private clusterConnectionService: ClusterConnectionService
    ) {
        const serviceProperties: any = request.reportingTask.component.properties;
        const properties: Property[] = Object.entries(serviceProperties).map((entry: any) => {
            const [property, value] = entry;
            return {
                property,
                value,
                descriptor: request.reportingTask.component.descriptors[property]
            };
        });

        const defaultSchedulingPeriod: any = request.reportingTask.component.defaultSchedulingPeriod;
        this.schedulingStrategy = request.reportingTask.component.schedulingStrategy;
        let schedulingPeriod: string;

        if (this.schedulingStrategy === 'CRON_DRIVEN') {
            this.cronDrivenSchedulingPeriod = request.reportingTask.component.schedulingPeriod;
            this.timerDrivenSchedulingPeriod = defaultSchedulingPeriod['TIMER_DRIVEN'];

            schedulingPeriod = this.cronDrivenSchedulingPeriod;
        } else {
            this.cronDrivenSchedulingPeriod = defaultSchedulingPeriod['CRON_DRIVEN'];
            this.timerDrivenSchedulingPeriod = request.reportingTask.component.schedulingPeriod;

            schedulingPeriod = this.timerDrivenSchedulingPeriod;
        }

        // build the form
        this.editReportingTaskForm = this.formBuilder.group({
            name: new FormControl(request.reportingTask.component.name, Validators.required),
            state: new FormControl(request.reportingTask.component.state === 'STOPPED', Validators.required),
            schedulingStrategy: new FormControl(
                request.reportingTask.component.schedulingStrategy,
                Validators.required
            ),
            schedulingPeriod: new FormControl(schedulingPeriod, Validators.required),
            properties: new FormControl(properties),
            comments: new FormControl(request.reportingTask.component.comments)
        });
    }

    formatType(entity: ReportingTaskEntity): string {
        return this.nifiCommon.formatType(entity.component);
    }

    formatBundle(entity: ReportingTaskEntity): string {
        return this.nifiCommon.formatBundle(entity.component.bundle);
    }

    submitForm(postUpdateNavigation?: string[]) {
        const payload: any = {
            revision: this.client.getRevision(this.request.reportingTask),
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                id: this.request.reportingTask.id,
                name: this.editReportingTaskForm.get('name')?.value,
                comments: this.editReportingTaskForm.get('comments')?.value,
                schedulingStrategy: this.editReportingTaskForm.get('schedulingStrategy')?.value,
                schedulingPeriod: this.editReportingTaskForm.get('schedulingPeriod')?.value,
                state: this.editReportingTaskForm.get('state')?.value ? 'STOPPED' : 'DISABLED'
            }
        };

        const propertyControl: AbstractControl | null = this.editReportingTaskForm.get('properties');
        if (propertyControl && propertyControl.dirty) {
            const properties: Property[] = propertyControl.value;
            const values: { [key: string]: string | null } = {};
            properties.forEach((property) => (values[property.property] = property.value));
            payload.component.properties = values;
            payload.component.sensitiveDynamicPropertyNames = properties
                .filter((property) => property.descriptor.dynamic && property.descriptor.sensitive)
                .map((property) => property.descriptor.name);
        }

        this.editReportingTask.next({
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

    schedulingStrategyChanged(value: string): void {
        this.schedulingStrategy = value;

        if (value === 'CRON_DRIVEN') {
            this.editReportingTaskForm.get('schedulingPeriod')?.setValue(this.cronDrivenSchedulingPeriod);
        } else {
            this.editReportingTaskForm.get('schedulingPeriod')?.setValue(this.timerDrivenSchedulingPeriod);
        }
    }

    schedulingPeriodChanged(): void {
        if (this.schedulingStrategy === 'CRON_DRIVEN') {
            this.cronDrivenSchedulingPeriod = this.editReportingTaskForm.get('schedulingPeriod')?.value;
        } else {
            this.timerDrivenSchedulingPeriod = this.editReportingTaskForm.get('schedulingPeriod')?.value;
        }
    }

    protected readonly TextTip = TextTip;
}
