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

import { Component, computed, input, OnDestroy, inject } from '@angular/core';
import { ErrorBanner } from '../../error-banner/error-banner.component';
type StatusVariant = 'critical' | 'warning' | 'info' | 'success';
import { ConnectorWizardStore } from '../connector-wizard.store';

/**
 * Shared context banner that reads banner errors from the ConnectorWizardStore.
 */
@Component({
    selector: 'wizard-context-banner',
    standalone: true,
    imports: [ErrorBanner],
    templateUrl: './wizard-context-banner.component.html',
    styleUrl: './wizard-context-banner.component.scss'
})
export class WizardContextBanner implements OnDestroy {
    private wizardStore = inject(ConnectorWizardStore);

    /** Preserved for API compatibility; ErrorBanner is always error-styled. */
    readonly variant = input<StatusVariant>('critical');
    readonly panelClass = input<string | undefined>('mb-4');
    readonly persistOnDestroy = input(false);

    readonly messages = computed(() => {
        const errors = this.wizardStore.bannerErrors();
        return errors.length > 0 ? errors : null;
    });

    ngOnDestroy(): void {
        if (!this.persistOnDestroy()) {
            this.dismiss();
        }
    }

    dismiss(): void {
        this.wizardStore.clearBannerErrors();
    }
}
