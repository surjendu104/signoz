import { TabsProps } from 'antd';
import TabLabel from 'components/TabLabel';
import { PANEL_TYPES } from 'constants/queryBuilder';
import TimeSeriesView from 'container/TimeSeriesView';
import ListView from 'container/TracesExplorer/ListView';
import TableView from 'container/TracesExplorer/TableView';
import TracesView from 'container/TracesExplorer/TracesView';
import { TagFilterItem } from 'types/api/queryBuilder/queryBuilderData';
import { DataSource } from 'types/common/queryBuilder';

interface GetTabsItemsProps {
	isListViewDisabled: boolean;
	isFilterApplied: boolean;
}

export const getTabsItems = ({
	isListViewDisabled,
	isFilterApplied,
}: GetTabsItemsProps): TabsProps['items'] => [
	{
		label: (
			<TabLabel
				label="List View"
				isDisabled={isListViewDisabled}
				tooltipText="Please remove attributes from Group By filter to switch to List View tab"
			/>
		),
		key: PANEL_TYPES.LIST,
		children: <ListView isFilterApplied={isFilterApplied} />,
		disabled: isListViewDisabled,
	},
	{
		label: (
			<TabLabel
				label="Traces"
				isDisabled={isListViewDisabled}
				tooltipText="Please remove attributes from Group By filter to switch to Traces tab"
			/>
		),
		key: PANEL_TYPES.TRACE,
		children: <TracesView isFilterApplied={isFilterApplied} />,
		disabled: isListViewDisabled,
	},
	{
		label: <TabLabel label="Time Series" isDisabled={false} />,
		key: PANEL_TYPES.TIME_SERIES,
		children: (
			<TimeSeriesView
				dataSource={DataSource.TRACES}
				isFilterApplied={isFilterApplied}
			/>
		),
	},
	{
		label: 'Table View',
		key: PANEL_TYPES.TABLE,
		children: <TableView />,
	},
];

export const isTagFilterItemEqual = (
	filterItem: TagFilterItem,
	selectedTag: TagFilterItem,
): boolean =>
	filterItem.key?.key === selectedTag.key?.key &&
	filterItem.op === selectedTag.op &&
	JSON.stringify(filterItem.value) === JSON.stringify(selectedTag.value);

export const isSelectedTagsAlreadyPresent = (
	filterItems: TagFilterItem[],
	selectedTags: TagFilterItem[],
): boolean =>
	selectedTags.every((selectedTag) =>
		filterItems.some((filterItem: TagFilterItem) =>
			isTagFilterItemEqual(filterItem, selectedTag),
		),
	);
