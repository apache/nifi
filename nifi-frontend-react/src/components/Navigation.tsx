import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { useSelector } from 'react-redux';
import { RootState } from '../state/store';
import { Settings, Activity, FileText, Users, Shield, BarChart3 } from 'lucide-react';
import clsx from 'clsx';

const Navigation: React.FC = () => {
  const location = useLocation();
  const currentUser = useSelector((state: RootState) => state.currentUser.user);
  
  const navigationItems = [
    {
      name: 'Flow Designer',
      href: '/nifi/flow-designer',
      icon: Activity,
      description: 'Design and manage data flows',
    },
    {
      name: 'Data Provenance',
      href: '/nifi/provenance',
      icon: FileText,
      description: 'Track data lineage and provenance',
    },
    {
      name: 'Controller Services',
      href: '/nifi/controller-services',
      icon: Settings,
      description: 'Manage shared services',
    },
    {
      name: 'Users',
      href: '/nifi/users',
      icon: Users,
      description: 'User and group management',
    },
    {
      name: 'Policies',
      href: '/nifi/policies',
      icon: Shield,
      description: 'Access control policies',
    },
    {
      name: 'Summary',
      href: '/nifi/summary',
      icon: BarChart3,
      description: 'System summary and statistics',
    },
  ];

  const isActive = (href: string) => location.pathname.startsWith(href);

  return (
    <nav className="bg-white shadow-sm border-b">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          <div className="flex items-center">
            <Link to="/nifi/flow-designer" className="flex items-center space-x-3">
              <div className="flex-shrink-0">
                <Activity className="h-8 w-8 text-primary-600" />
              </div>
              <div className="text-xl font-bold text-gray-900">Apache NiFi</div>
            </Link>
          </div>
          
          <div className="hidden md:block">
            <div className="ml-10 flex items-baseline space-x-1">
              {navigationItems.map((item) => {
                const Icon = item.icon;
                return (
                  <Link
                    key={item.name}
                    to={item.href}
                    className={clsx(
                      'group flex items-center px-3 py-2 rounded-md text-sm font-medium transition-colors',
                      isActive(item.href)
                        ? 'bg-primary-100 text-primary-700'
                        : 'text-gray-600 hover:text-gray-900 hover:bg-gray-100'
                    )}
                    title={item.description}
                  >
                    <Icon className="h-4 w-4 mr-2" />
                    {item.name}
                  </Link>
                );
              })}
            </div>
          </div>

          <div className="flex items-center space-x-4">
            {currentUser && (
              <div className="flex items-center space-x-3">
                <div className="text-sm">
                  <div className="font-medium text-gray-900">{currentUser.identity}</div>
                  <div className="text-gray-500">Authenticated</div>
                </div>
                <div className="h-8 w-8 bg-primary-600 rounded-full flex items-center justify-center text-white text-sm font-medium">
                  {currentUser.identity?.charAt(0)?.toUpperCase() || 'U'}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
      
      {/* Mobile menu */}
      <div className="md:hidden">
        <div className="px-2 pt-2 pb-3 space-y-1 sm:px-3 bg-white border-t">
          {navigationItems.map((item) => {
            const Icon = item.icon;
            return (
              <Link
                key={item.name}
                to={item.href}
                className={clsx(
                  'group flex items-center px-3 py-2 rounded-md text-base font-medium transition-colors',
                  isActive(item.href)
                    ? 'bg-primary-100 text-primary-700'
                    : 'text-gray-600 hover:text-gray-900 hover:bg-gray-100'
                )}
              >
                <Icon className="h-5 w-5 mr-3" />
                <div>
                  <div>{item.name}</div>
                  <div className="text-xs text-gray-500">{item.description}</div>
                </div>
              </Link>
            );
          })}
        </div>
      </div>
    </nav>
  );
};

export default Navigation;