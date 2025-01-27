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

import { Component, Input } from '@angular/core';
import { MatTreeModule } from '@angular/material/tree';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { NgClass, NgTemplateOutlet } from '@angular/common';
import { RouterLink } from '@angular/router';
import { MatDialogModule } from '@angular/material/dialog';
import { BulletinsTipInput, ValidationErrorsTipInput } from '../../../state/shared';
import {
    AffectedComponent,
    AffectedComponentEntity,
    NiFiCommon,
    NifiTooltipDirective,
    ProcessGroupName
} from '@nifi/shared';
import { ValidationErrorsTip } from '../tooltips/validation-errors-tip/validation-errors-tip.component';
import { BulletinsTip } from '../tooltips/bulletins-tip/bulletins-tip.component';

@Component({
    selector: 'parameter-references',
    templateUrl: './parameter-references.component.html',
    imports: [
        MatTreeModule,
        MatIconModule,
        MatButtonModule,
        NgTemplateOutlet,
        NgClass,
        NifiTooltipDirective,
        RouterLink,
        MatDialogModule
    ],
    styleUrls: ['./parameter-references.component.scss']
})
export class ParameterReferences {
    @Input() disabledLinks: boolean = false;
    @Input() set parameterReferences(parameterReferences: AffectedComponentEntity[] | undefined) {
        // reset existing state
        this.processGroups = [];
        this.parameterReferenceMap.clear();

        if (parameterReferences) {
            parameterReferences.forEach((parameterReference) => {
                const pgId: string = parameterReference.processGroup.id;

                if (!this.processGroups.some((pg) => pg.id == pgId)) {
                    this.processGroups.push(parameterReference.processGroup);
                }

                if (this.parameterReferenceMap.has(pgId)) {
                    // @ts-ignore
                    this.parameterReferenceMap.get(pgId).push(parameterReference);
                } else {
                    this.parameterReferenceMap.set(pgId, [parameterReference]);
                }
            });
        }
    }

    protected readonly ValidationErrorsTip = ValidationErrorsTip;
    protected readonly BulletinsTip = BulletinsTip;

    // parameter references lookup by process group id
    parameterReferenceMap: Map<string, AffectedComponentEntity[]> = new Map<string, AffectedComponentEntity[]>();
    processGroups: ProcessGroupName[] = [];

    constructor(private nifiCommon: NiFiCommon) {}

    getUnauthorized(references: AffectedComponentEntity[]) {
        return references.filter((reference) => !reference.permissions.canRead);
    }

    getReferencesByType(references: AffectedComponentEntity[], referenceType: string) {
        return references.filter(
            (reference) => reference.permissions.canRead && reference.component.referenceType == referenceType
        );
    }

    isServiceInvalid(reference: AffectedComponent): boolean {
        return reference.state == 'DISABLED' && !this.nifiCommon.isEmpty(reference.validationErrors);
    }

    isNonServiceInvalid(reference: AffectedComponent): boolean {
        return reference.state == 'STOPPED' && !this.nifiCommon.isEmpty(reference.validationErrors);
    }

    getValidationErrorTipData(reference: AffectedComponent): ValidationErrorsTipInput {
        return {
            isValidating: false,
            validationErrors: reference.validationErrors
        };
    }

    getNonServiceStateIcon(reference: AffectedComponent): string {
        if (reference.state == 'STOPPED') {
            return 'stopped fa fa-stop error-color-variant';
        } else if (reference.state == 'RUNNING') {
            return 'running fa fa-play success-color-default';
        } else {
            return 'disabled icon icon-enable-false neutral-color';
        }
    }

    getServiceStateIcon(reference: AffectedComponent): string {
        if (reference.state == 'ENABLED') {
            return 'enabled fa fa-flash success-color-variant';
        } else {
            return 'disabled icon icon-enable-false neutral-color';
        }
    }

    getRouteForReference(reference: AffectedComponent): string[] {
        if (reference.referenceType === 'CONTROLLER_SERVICE') {
            if (reference.processGroupId == null) {
                return ['/settings', 'management-controller-services', reference.id];
            } else {
                return ['/process-groups', reference.processGroupId, 'controller-services', reference.id];
            }
        } else {
            return ['/process-groups', reference.processGroupId, 'processors', reference.id];
        }
    }

    hasBulletins(entity: AffectedComponentEntity): boolean {
        return !this.nifiCommon.isEmpty(entity.bulletins);
    }

    getBulletinsTipData(entity: AffectedComponentEntity): BulletinsTipInput {
        return {
            bulletins: entity.bulletins
        };
    }

    hasActiveThreads(reference: AffectedComponent): boolean {
        return reference.activeThreadCount != null && reference.activeThreadCount > 0;
    }
}
