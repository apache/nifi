// ControllerServiceStateService migrated from Angular with TypeScript and RxJS
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

import clientService, { RevisionHolder } from './clientService';
import { stripProtocol } from './componentStateService';
import { isDisconnectionAcknowledged } from './clusterConnectionService';

export const API = '../nifi-api';

export interface ControllerServiceReferencingComponent extends RevisionHolder {
  id: string;
  referenceType: 'ControllerService' | 'Processor' | 'ReportingTask' | string;
  referencingComponents?: ControllerServiceReferencingComponentEntity[];
}

export interface ControllerServiceReferencingComponentEntity extends RevisionHolder {
  id: string;
  component: ControllerServiceReferencingComponent;
}

export interface ControllerServiceComponent {
  referencingComponents?: ControllerServiceReferencingComponentEntity[];
}

export interface ControllerServiceEntity extends RevisionHolder {
  id: string;
  uri: string;
  component: ControllerServiceComponent;
}

export function getControllerService$(id: string): Observable<unknown> {
  const url = `${API}/controller-services/${id}`;
  const params = new URLSearchParams({ uiOnly: 'true' });
  return ajax.getJSON(`${url}?${params.toString()}`).pipe(map((response) => response));
}

export function setEnable$(controllerService: ControllerServiceEntity, enabled: boolean): Observable<unknown> {
  const url = `${stripProtocol(controllerService.uri)}/run-status`;
  return ajax({
    url,
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: {
      revision: clientService.getRevision(controllerService),
      disconnectedNodeAcknowledged: isDisconnectionAcknowledged(),
      state: enabled ? 'ENABLED' : 'DISABLED',
      uiOnly: true
    }
  }).pipe(map((response) => response.response));
}

export function updateReferencingServices$(
  controllerService: ControllerServiceEntity,
  enabled: boolean
): Observable<unknown> {
  const referencingComponentRevisions: Record<string, unknown> = {};
  collectReferencingComponentRevisions(
    controllerService.component.referencingComponents ?? [],
    referencingComponentRevisions,
    true
  );

  const url = `${stripProtocol(controllerService.uri)}/references`;
  return ajax({
    url,
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: {
      id: controllerService.id,
      state: enabled ? 'ENABLED' : 'DISABLED',
      referencingComponentRevisions,
      disconnectedNodeAcknowledged: isDisconnectionAcknowledged(),
      uiOnly: true
    }
  }).pipe(map((response) => response.response));
}

export function updateReferencingSchedulableComponents$(
  controllerService: ControllerServiceEntity,
  running: boolean
): Observable<unknown> {
  const referencingComponentRevisions: Record<string, unknown> = {};
  collectReferencingComponentRevisions(
    controllerService.component.referencingComponents ?? [],
    referencingComponentRevisions,
    false
  );

  const url = `${stripProtocol(controllerService.uri)}/references`;
  return ajax({
    url,
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: {
      id: controllerService.id,
      state: running ? 'RUNNING' : 'STOPPED',
      referencingComponentRevisions,
      disconnectedNodeAcknowledged: isDisconnectionAcknowledged(),
      uiOnly: true
    }
  }).pipe(map((response) => response.response));
}

function collectReferencingComponentRevisions(
  referencingComponents: ControllerServiceReferencingComponentEntity[],
  referencingComponentRevisions: Record<string, unknown>,
  serviceOnly: boolean
): void {
  referencingComponents.forEach((entity) => {
    const component = entity.component;

    if (!component) {
      return;
    }

    if (serviceOnly) {
      if (component.referenceType === 'ControllerService') {
        referencingComponentRevisions[component.id] = clientService.getRevision(entity);
      }
    } else if (component.referenceType === 'Processor' || component.referenceType === 'ReportingTask') {
      referencingComponentRevisions[component.id] = clientService.getRevision(entity);
    }

    if (component.referencingComponents && component.referencingComponents.length > 0) {
      collectReferencingComponentRevisions(component.referencingComponents, referencingComponentRevisions, serviceOnly);
    }
  });
}
