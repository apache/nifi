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
import { MatMenuModule } from '@angular/material/menu';
import {
    AbstractControl,
    FormBuilder,
    FormControl,
    FormGroup,
    ReactiveFormsModule,
    ValidationErrors,
    ValidatorFn,
    Validators
} from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe, NgClass } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { Observable, of } from 'rxjs';
import {
    BulletinsTipInput,
    InlineServiceCreationRequest,
    InlineServiceCreationResponse,
    ParameterContextEntity,
    Property
} from '../../../../../../../state/shared';
import { Client } from '../../../../../../../service/client.service';
import {
    DisableComponentRequest,
    EditComponentDialogRequest,
    EnableComponentRequest,
    StartComponentRequest,
    StopComponentRequest,
    UpdateProcessorRequest
} from '../../../../../state/flow';
import { PropertyTable } from '../../../../../../../ui/common/property-table/property-table.component';
import { NifiSpinnerDirective } from '../../../../../../../ui/common/spinner/nifi-spinner.directive';
import {
    ComponentType,
    SelectOption,
    NifiTooltipDirective,
    NiFiCommon,
    TextTip,
    CopyDirective,
    Revision,
    BulletinEntity
} from '@nifi/shared';
import { RunDurationSlider } from './run-duration-slider/run-duration-slider.component';
import {
    RelationshipConfiguration,
    RelationshipSettings
} from './relationship-settings/relationship-settings.component';
import { ClusterConnectionService } from '../../../../../../../service/cluster-connection.service';
import { CanvasUtils } from '../../../../../service/canvas-utils.service';
import { ConvertToParameterResponse } from '../../../../../service/parameter-helper.service';
import { PropertyVerification } from '../../../../../../../ui/common/property-verification/property-verification.component';
import {
    ConfigVerificationResult,
    ModifiedProperties,
    VerifyPropertiesRequestContext
} from '../../../../../../../state/property-verification';
import { TabbedDialog } from '../../../../../../../ui/common/tabbed-dialog/tabbed-dialog.component';
import { ErrorContextKey } from '../../../../../../../state/error';
import { ContextErrorBanner } from '../../../../../../../ui/common/context-error-banner/context-error-banner.component';
import { BulletinsTip } from '../../../../../../../ui/common/tooltips/bulletins-tip/bulletins-tip.component';
import { ConnectedPosition } from '@angular/cdk/overlay';

@Component({
    selector: 'edit-processor',
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
        MatMenuModule,
        AsyncPipe,
        PropertyTable,
        NifiSpinnerDirective,
        NifiTooltipDirective,
        RunDurationSlider,
        RelationshipSettings,
        PropertyVerification,
        ContextErrorBanner,
        CopyDirective,
        NgClass
    ],
    styleUrls: ['./edit-processor.component.scss']
})
export class EditProcessor extends TabbedDialog {
    @Input() set processorUpdates(processorUpdates: any | undefined) {
        this.processRunStateUpdates(processorUpdates);
    }
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
    @Input() saving$!: Observable<boolean>;

    @Input() propertyVerificationResults$!: Observable<ConfigVerificationResult[]>;
    @Input() propertyVerificationStatus$: Observable<'pending' | 'loading' | 'success'> = of('pending');

    @Output() verify: EventEmitter<VerifyPropertiesRequestContext> = new EventEmitter<VerifyPropertiesRequestContext>();
    @Output() editProcessor: EventEmitter<UpdateProcessorRequest> = new EventEmitter<UpdateProcessorRequest>();
    @Output() stopComponentRequest: EventEmitter<StopComponentRequest> = new EventEmitter<StopComponentRequest>();
    @Output() startComponentRequest: EventEmitter<StartComponentRequest> = new EventEmitter<StartComponentRequest>();
    @Output() disableComponentRequest: EventEmitter<DisableComponentRequest> =
        new EventEmitter<DisableComponentRequest>();
    @Output() enableComponentRequest: EventEmitter<EnableComponentRequest> = new EventEmitter<EnableComponentRequest>();

    protected readonly TextTip = TextTip;
    protected readonly BulletinsTip = BulletinsTip;

    editProcessorForm: FormGroup;
    readonly: boolean = true;
    status: any;
    revision!: Revision;
    bulletins!: BulletinEntity[];

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
        private canvasUtils: CanvasUtils,
        private clusterConnectionService: ClusterConnectionService,
        private nifiCommon: NiFiCommon
    ) {
        super('edit-processor-selected-index');

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
            concurrentTasks: new FormControl(concurrentTasks, [Validators.required, Validators.min(1)]),
            schedulingPeriod: new FormControl(schedulingPeriod, Validators.required),
            executionNode: new FormControl(request.entity.component.config.executionNode, Validators.required),
            properties: new FormControl({ value: properties, disabled: this.readonly }),
            relationshipConfiguration: new FormControl({ value: relationshipConfiguration, disabled: this.readonly }, [
                Validators.required,
                this.relationshipConfigurationValidator()
            ]),
            comments: new FormControl(request.entity.component.config.comments)
        });

        if (this.supportsBatching()) {
            this.editProcessorForm.addControl(
                'runDuration',
                new FormControl({ value: this.runDurationMillis, disabled: this.readonly }, Validators.required)
            );
        }

        this.processRunStateUpdates(request.entity);
    }

    private processRunStateUpdates(entity: any) {
        this.status = entity.status;
        this.revision = entity.revision;
        this.bulletins = entity.bulletins;

        this.readonly = !entity.permissions.canWrite || !this.canvasUtils.runnableSupportsModification(entity);

        if (this.readonly) {
            this.editProcessorForm.get('properties')?.disable();
            this.editProcessorForm.get('relationshipConfiguration')?.disable();

            if (this.supportsBatching()) {
                this.editProcessorForm.get('runDuration')?.disable();
            }
        } else {
            this.editProcessorForm.get('properties')?.enable();
            this.editProcessorForm.get('relationshipConfiguration')?.enable();

            if (this.supportsBatching()) {
                this.editProcessorForm.get('runDuration')?.enable();
            }
        }
    }

    private relationshipConfigurationValidator(): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const relationshipConfiguration: RelationshipConfiguration = control.value;
            const retried: string[] = relationshipConfiguration.relationships
                .filter((relationship) => relationship.retry)
                .map((relationship) => relationship.name);

            if (retried.length > 0) {
                let hasErrors = false;
                const errors: ValidationErrors = {};

                if (relationshipConfiguration.retryCount == null || relationshipConfiguration.retryCount < 0) {
                    errors['invalidRetry'] = true;
                    hasErrors = true;
                }
                if (this.nifiCommon.isBlank(relationshipConfiguration.maxBackoffPeriod)) {
                    errors['invalidBackoff'] = true;
                    hasErrors = true;
                }

                if (hasErrors) {
                    return errors;
                }
            }

            return null;
        };
    }

    supportsBatching(): boolean {
        return this.request.entity.component.supportsBatching == true;
    }

    formatType(): string {
        return this.nifiCommon.formatType(this.request.entity.component);
    }

    formatBundle(): string {
        return this.nifiCommon.formatBundle(this.request.entity.component.bundle);
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

    submitForm(postUpdateNavigation?: string[], postUpdateNavigationBoundary?: string[]) {
        const relationshipConfiguration: RelationshipConfiguration =
            this.editProcessorForm.get('relationshipConfiguration')?.value;
        const autoTerminated: string[] = relationshipConfiguration.relationships
            .filter((relationship) => relationship.autoTerminate)
            .map((relationship) => relationship.name);
        const retried: string[] = relationshipConfiguration.relationships
            .filter((relationship) => relationship.retry)
            .map((relationship) => relationship.name);

        const payload: any = {
            revision: this.client.getRevision({
                ...this.request.entity,
                revision: this.revision
            }),
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
            payload.component.config.properties = this.getModifiedProperties();
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
            id: this.request.entity.id,
            uri: this.request.entity.uri,
            type: ComponentType.Processor,
            errorStrategy: 'banner',
            postUpdateNavigation,
            postUpdateNavigationBoundary,
            payload
        });
    }

    hasBulletins(): boolean {
        return this.request.entity.permissions.canRead && !this.nifiCommon.isEmpty(this.bulletins);
    }

    getBulletinsTipData(): BulletinsTipInput {
        return {
            bulletins: this.bulletins
        };
    }

    getBulletinTooltipPosition(): ConnectedPosition {
        return {
            originX: 'end',
            originY: 'bottom',
            overlayX: 'end',
            overlayY: 'top',
            offsetX: -8,
            offsetY: 8
        };
    }

    getMostSevereBulletinLevel(): string | null {
        // determine the most severe of the bulletins
        const mostSevere = this.canvasUtils.getMostSevereBulletin(this.bulletins);
        return mostSevere ? mostSevere.bulletin.level.toLowerCase() : null;
    }

    isStoppable(): boolean {
        return this.status.aggregateSnapshot.runStatus === 'Running';
    }

    isStopping(): boolean {
        return (
            this.status.aggregateSnapshot.runStatus === 'Stopped' && this.status.aggregateSnapshot.activeThreadCount > 0
        );
    }

    isValidating(): boolean {
        return this.status.aggregateSnapshot.runStatus === 'Validating';
    }

    isInvalid(): boolean {
        return this.status.aggregateSnapshot.runStatus === 'Invalid';
    }

    isDisabled(): boolean {
        return this.status.aggregateSnapshot.runStatus === 'Disabled';
    }

    isRunnable(): boolean {
        return (
            !(
                this.status.aggregateSnapshot.runStatus === 'Running' ||
                this.status.aggregateSnapshot.activeThreadCount > 0
            ) && this.status.aggregateSnapshot.runStatus === 'Stopped'
        );
    }

    isDisableable(): boolean {
        return (
            !(
                this.status.aggregateSnapshot.runStatus === 'Running' ||
                this.status.aggregateSnapshot.activeThreadCount > 0
            ) &&
            (this.status.aggregateSnapshot.runStatus === 'Stopped' ||
                this.status.aggregateSnapshot.runStatus === 'Invalid')
        );
    }

    isEnableable(): boolean {
        return (
            !(
                this.status.aggregateSnapshot.runStatus === 'Running' ||
                this.status.aggregateSnapshot.activeThreadCount > 0
            ) && this.status.aggregateSnapshot.runStatus === 'Disabled'
        );
    }

    canOperate(): boolean {
        return this.request.entity.permissions.canWrite || this.request.entity.operatePermissions?.canWrite;
    }

    stop() {
        this.stopComponentRequest.next({
            id: this.request.entity.id,
            uri: this.request.entity.uri,
            type: ComponentType.Processor,
            revision: this.client.getRevision({
                ...this.request.entity,
                revision: this.revision
            }),
            errorStrategy: 'snackbar'
        });
    }

    start() {
        this.startComponentRequest.next({
            id: this.request.entity.id,
            uri: this.request.entity.uri,
            type: ComponentType.Processor,
            revision: this.client.getRevision({
                ...this.request.entity,
                revision: this.revision
            }),
            errorStrategy: 'snackbar'
        });
    }

    disable() {
        this.disableComponentRequest.next({
            id: this.request.entity.id,
            uri: this.request.entity.uri,
            type: ComponentType.Processor,
            revision: this.client.getRevision({
                ...this.request.entity,
                revision: this.revision
            }),
            errorStrategy: 'snackbar'
        });
    }

    enable() {
        this.enableComponentRequest.next({
            id: this.request.entity.id,
            uri: this.request.entity.uri,
            type: ComponentType.Processor,
            revision: this.client.getRevision({
                ...this.request.entity,
                revision: this.revision
            }),
            errorStrategy: 'snackbar'
        });
    }

    private getModifiedProperties(): ModifiedProperties {
        const propertyControl: AbstractControl | null = this.editProcessorForm.get('properties');
        if (propertyControl && propertyControl.dirty) {
            const properties: Property[] = propertyControl.value;
            const values: { [key: string]: string | null } = {};
            properties.forEach((property) => (values[property.property] = property.value));
            return values;
        }
        return {};
    }

    override isDirty(): boolean {
        return this.editProcessorForm.dirty;
    }

    verifyClicked(entity: any): void {
        this.verify.next({
            entity,
            properties: this.getModifiedProperties()
        });
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
