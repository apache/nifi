import { ReactNode } from 'react';
import { Navigate } from 'react-router-dom';
import { useAppSelector } from './redux';
import type { CurrentUser } from '../state/currentUserSlice';

export interface AuthorizationGuardProps {
  check: (user?: CurrentUser) => boolean;
  fallbackUrl?: string;
  children: ReactNode;
}

export function withAuthorizationGuard({
  user,
  check,
  fallbackUrl,
  onUnauthorized
}: {
  user?: CurrentUser;
  check: (user?: CurrentUser) => boolean;
  fallbackUrl?: string;
  onUnauthorized?: () => void;
}): ReactNode {
  if (check(user)) {
    return null;
  }

  if (onUnauthorized) {
    onUnauthorized();
  }

  if (fallbackUrl) {
    return <Navigate to={fallbackUrl} replace />;
  }

  return <Navigate to="/error" replace />;
}

export const AuthorizationGate = ({ check, fallbackUrl, children }: AuthorizationGuardProps) => {
  const user = useAppSelector((state) => state.currentUser.user);

  if (!check(user)) {
    if (fallbackUrl) {
      return <Navigate to={fallbackUrl} replace />;
    }

    return (
      <div className="authorization-error">
        <h2>Unable to load</h2>
        <p>Authorization check failed. Contact the system administrator.</p>
      </div>
    );
  }

  return <>{children}</>;
};
