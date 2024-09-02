import './TracesModulePage.styles.scss';

import RouteTab from 'components/RouteTab';
import { TabRoutes } from 'components/RouteTab/types';
import ROUTES from 'constants/routes';
import ResourceAttributesFilter from 'container/ResourceAttributesFilter';
import history from 'lib/history';
import { useLocation } from 'react-router-dom';

import { tracesExplorer, tracesSaveView } from './constants';

function TracesModulePage(): JSX.Element {
	const { pathname } = useLocation();

	const routes: TabRoutes[] = [tracesExplorer, tracesSaveView];

	return (
		<div className="traces-module-container">
			{pathname === ROUTES.TRACES_EXPLORER && (
				<div className="resources-filter">
					<ResourceAttributesFilter />
				</div>
			)}
			<RouteTab routes={routes} activeKey={pathname} history={history} />
		</div>
	);
}

export default TracesModulePage;
