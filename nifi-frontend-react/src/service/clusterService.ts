// ClusterService migrated from Angular with RxJS
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

export const API = '../nifi-api';

export interface ClusterSummaryResponse {
  // Define properties based on expected API response
  summary?: any;
}

export interface ClusterSearchResults {
  // Define properties based on expected API response
  results?: any;
}

export function getClusterSummary$(): Observable<ClusterSummaryResponse> {
  return ajax.getJSON<ClusterSummaryResponse>(`${API}/flow/cluster/summary`).pipe(
    map(response => response)
  );
}

export function searchCluster$(q?: string): Observable<ClusterSearchResults> {
  const params = q ? `?q=${encodeURIComponent(q)}` : '';
  return ajax.getJSON<ClusterSearchResults>(`${API}/flow/cluster/search-results${params}`).pipe(
    map(response => response)
  );
}
