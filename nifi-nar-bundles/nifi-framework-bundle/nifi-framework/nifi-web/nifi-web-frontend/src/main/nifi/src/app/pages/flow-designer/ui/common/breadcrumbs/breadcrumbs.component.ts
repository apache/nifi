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

import { Component, ElementRef, Input, ViewChild } from '@angular/core';
import { initialState } from '../../../state/flow/flow.reducer';

import { RouterLink } from '@angular/router';
import { BreadcrumbEntity } from '../../../state/shared';

@Component({
    selector: 'breadcrumbs',
    standalone: true,
    templateUrl: './breadcrumbs.component.html',
    imports: [RouterLink],
    styleUrls: ['./breadcrumbs.component.scss']
})
export class Breadcrumbs {
    @Input() entity: BreadcrumbEntity = initialState.flow.processGroupFlow.breadcrumb;
    @Input() currentProcessGroupId: string = initialState.id;

    private scrolledToProcessGroupId = '';

    @ViewChild('currentProcessGroup') set currentProcessGroupBreadcrumb(currentProcessGroupBreadcrumb: ElementRef) {
        // only auto scroll to the breadcrumb for the current pg once as the user may have manually scrolled since
        if (currentProcessGroupBreadcrumb && this.scrolledToProcessGroupId != this.currentProcessGroupId) {
            currentProcessGroupBreadcrumb.nativeElement.scrollIntoView();
            this.scrolledToProcessGroupId = this.currentProcessGroupId;
        }
    }

    prepareBreadcrumbs(): BreadcrumbEntity[] {
        const breadcrumbs: BreadcrumbEntity[] = [];
        this.prepareBreadcrumb(breadcrumbs, this.entity);
        return breadcrumbs.reverse();
    }

    prepareBreadcrumb(breadcrumbs: BreadcrumbEntity[], breadcrumbEntity: BreadcrumbEntity): void {
        breadcrumbs.push(breadcrumbEntity);
        if (breadcrumbEntity.parentBreadcrumb) {
            this.prepareBreadcrumb(breadcrumbs, breadcrumbEntity.parentBreadcrumb);
        }
    }

    isCurrentProcessGroupBreadcrumb(breadcrumb: BreadcrumbEntity): boolean {
        if (breadcrumb.id == this.currentProcessGroupId) {
            return true;
        }

        if (breadcrumb.parentBreadcrumb) {
            return this.currentProcessGroupId == 'root';
        }

        return false;
    }

    getVersionControlClass(breadcrumbEntity: BreadcrumbEntity): string {
        const vciState: string = breadcrumbEntity.versionedFlowState;
        if (vciState) {
            if (vciState === 'SYNC_FAILURE') {
                return 'sync-failure nifi-surface-default fa fa-question';
            } else if (vciState === 'LOCALLY_MODIFIED_AND_STALE') {
                return 'locally-modified-and-stale nifi-warn-lighter fa fa-exclamation-circle';
            } else if (vciState === 'STALE') {
                return 'stale nifi-warn-lighter fa fa-arrow-circle-up';
            } else if (vciState === 'LOCALLY_MODIFIED') {
                return 'locally-modified nifi-surface-default fa fa-asterisk';
            } else {
                // up to date
                return 'up-to-date nifi-success-default fa fa-check';
            }
        } else {
            return '';
        }
    }

    getVersionControlTooltip(breadcrumbEntity: BreadcrumbEntity): string {
        if (breadcrumbEntity.permissions.canRead && breadcrumbEntity.breadcrumb.versionControlInformation) {
            return breadcrumbEntity.breadcrumb.versionControlInformation.stateExplanation;
        } else {
            return 'This Process Group is not under version control.';
        }
    }

    getBreadcrumbLabel(breadcrumbEntity: BreadcrumbEntity): string {
        if (breadcrumbEntity.permissions.canRead) {
            return breadcrumbEntity.breadcrumb.name;
        }

        return breadcrumbEntity.id;
    }
}
