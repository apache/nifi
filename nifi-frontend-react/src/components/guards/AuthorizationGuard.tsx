import { ReactNode } from 'react';
import { Navigate } from 'react-router-dom';
import { useAppSelector } from '../../hooks/redux';
import type { CurrentUser } from '../../state/currentUserSlice';

export interface AuthorizationGuardProps {
  check: (user?: CurrentUser) => boolean;
  fallbackUrl?: string;
  loadingFallback?: ReactNode;
  children: ReactNode;
}

const AuthorizationGuard = ({
  check,
  fallbackUrl,
  loadingFallback = null,
  children
}: AuthorizationGuardProps) => {
  const { user, status } = useAppSelector((state) => state.currentUser);

  if (status === 'loading' || status === 'idle') {
    return <>{loadingFallback}</>;
  }

  const authorized = check(user);

  if (authorized) {
    return <>{children}</>;
  }

  if (fallbackUrl) {
    return <Navigate to={fallbackUrl} replace />;
  }

  return (
    <div className="authorization-error">
      <h2>Unable to load</h2>
      <p>Authorization check failed. Contact the system administrator.</p>
    </div>
  );
};

export default AuthorizationGuard;
