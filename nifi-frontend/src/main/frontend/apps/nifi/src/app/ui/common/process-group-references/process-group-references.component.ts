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
import { NgTemplateOutlet } from '@angular/common';
import { RouterLink } from '@angular/router';
import { MatDialogModule } from '@angular/material/dialog';
import { BoundProcessGroup } from '../../../state/shared';

@Component({
    selector: 'process-group-references',
    templateUrl: './process-group-references.component.html',
    imports: [MatTreeModule, MatIconModule, MatButtonModule, NgTemplateOutlet, RouterLink, MatDialogModule],
    styleUrls: ['./process-group-references.component.scss']
})
export class ProcessGroupReferences {
    @Input() set processGroupReferences(processGroupReferences: BoundProcessGroup[] | undefined) {
        this.authorizedProcessGroupReferences = this.getAuthorized(processGroupReferences);
        this.unauthorizedProcessGroupReferences = this.getUnauthorized(processGroupReferences);
    }
    @Input() disabledLinks = false;

    authorizedProcessGroupReferences: BoundProcessGroup[] = [];
    unauthorizedProcessGroupReferences: BoundProcessGroup[] = [];

    private getUnauthorized(references: BoundProcessGroup[] | undefined) {
        if (references) {
            return references.filter((reference) => !reference.permissions.canRead);
        } else {
            return [];
        }
    }

    private getAuthorized(references: BoundProcessGroup[] | undefined) {
        if (references) {
            return references.filter((reference) => reference.permissions.canRead);
        } else {
            return [];
        }
    }

    hasBoundProcessGroups(): boolean {
        return this.unauthorizedProcessGroupReferences.length + this.authorizedProcessGroupReferences.length > 0;
    }

    getRouteForReference(reference: BoundProcessGroup): string[] {
        if (reference.component.parentGroupId == null) {
            return ['/process-groups', reference.id];
        } else {
            return ['/process-groups', reference.component.parentGroupId, 'process-groups', reference.id];
        }
    }
}
