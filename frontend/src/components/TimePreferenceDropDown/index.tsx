import './TimePreference.styles.scss';

import { DownOutlined } from '@ant-design/icons';
import { Button, Dropdown, Radio, Typography } from 'antd';
import TimeItems, {
	timePreferance,
	timePreferenceType,
} from 'container/NewWidget/RightContainer/timeItems';
import { Globe } from 'lucide-react';
import { Dispatch, SetStateAction, useCallback, useMemo } from 'react';

import { menuItems } from './config';

function TimeFormatToggle(props: TimeFormatToggleProps): JSX.Element {
	const { timeFormat, setTimeFormat } = props;

	const handleToggle = (e: any): void => {
		setTimeFormat?.(e.target.value);
	};

	return (
		<Radio.Group
			onChange={handleToggle}
			value={timeFormat || '12H'}
			buttonStyle="solid"
		>
			<Radio.Button value="24H">24H</Radio.Button>
			<Radio.Button value="12H">12H</Radio.Button>
		</Radio.Group>
	);
}

function TimePreference({
	setSelectedTime,
	selectedTime,
	timeFormat,
	setTimeFormat,
}: TimePreferenceDropDownProps): JSX.Element {
	const timeMenuItemOnChangeHandler = useCallback(
		(event: TimeMenuItemOnChangeHandlerEvent) => {
			const selectedTime = TimeItems.find((e) => e.enum === event.key);
			if (selectedTime !== undefined) {
				setSelectedTime(selectedTime);
			}
		},
		[setSelectedTime],
	);

	const menu = useMemo(
		() => ({
			items: menuItems,
			onClick: timeMenuItemOnChangeHandler,
		}),
		[timeMenuItemOnChangeHandler],
	);

	return (
		<div>
			<Dropdown
				menu={menu}
				rootClassName="time-selection-menu"
				className="time-selection-target"
				trigger={['click']}
			>
				<Button>
					<div className="button-selected-text">
						<Globe size={14} />
						<Typography.Text className="selected-value">
							{selectedTime.name}
						</Typography.Text>
					</div>
					<DownOutlined />
				</Button>
			</Dropdown>
			<TimeFormatToggle timeFormat={timeFormat} setTimeFormat={setTimeFormat} />
		</div>
	);
}

interface TimeMenuItemOnChangeHandlerEvent {
	key: timePreferenceType | string;
}

interface TimePreferenceDropDownProps {
	setSelectedTime: Dispatch<SetStateAction<timePreferance>>;
	selectedTime: timePreferance;
	timeFormat?: '24H' | '12H';
	setTimeFormat?: Dispatch<SetStateAction<'24H' | '12H'>>;
}

interface TimeFormatToggleProps {
	timeFormat?: '24H' | '12H';
	setTimeFormat?: Dispatch<SetStateAction<'24H' | '12H'>>;
}

TimePreference.defaultProps = {
	setTimeFormat: undefined,
	timeFormat: '12H',
};

TimeFormatToggle.defaultProps = {
	setTimeFormat: undefined,
	timeFormat: '12H',
};

export default TimePreference;
