import { useSelector } from 'react-redux';

export function useDisconnectionAcknowledged() {
  return useSelector(state => state.clusterSummary.disconnectionAcknowledged);
}
