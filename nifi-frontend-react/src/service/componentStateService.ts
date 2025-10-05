// ComponentStateService migrated from Angular with TypeScript and RxJS
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

export interface LoadComponentStateRequest {
  componentUri: string;
}

export interface ClearComponentStateRequest {
  componentUri: string;
}

export interface ComponentStateEntity {
  componentState?: Record<string, unknown>;
  [key: string]: unknown;
}

export function getComponentState$(
  request: LoadComponentStateRequest
): Observable<ComponentStateEntity> {
  const url = `${stripProtocol(request.componentUri)}/state`;
  return ajax.getJSON<ComponentStateEntity>(url).pipe(map((response) => response));
}

export function clearComponentState$(
  request: ClearComponentStateRequest
): Observable<unknown> {
  const url = `${stripProtocol(request.componentUri)}/state/clear-requests`;
  return ajax({
    url,
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: {}
  }).pipe(map((response) => response.response));
}

export function clearComponentStateEntry$(
  componentUri: string,
  componentStateEntity: ComponentStateEntity
): Observable<unknown> {
  const url = `${stripProtocol(componentUri)}/state/clear-requests`;
  return ajax({
    url,
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: componentStateEntity
  }).pipe(map((response) => response.response));
}

export function stripProtocol(uri: string): string {
  return uri.replace(/^https?:\/\//, '');
}
