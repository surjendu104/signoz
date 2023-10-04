import {
	checkCommaInValue,
	getTagToken,
} from 'container/QueryBuilder/filters/QueryBuilderSearch/utils';
import { Option } from 'container/QueryBuilder/type';
import { transformStringWithPrefix } from 'lib/query/transformStringWithPrefix';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { BaseAutocompleteData } from 'types/api/queryBuilder/queryAutocompleteResponse';

import { WhereClauseConfig } from './useAutoComplete';
import { useOperators } from './useOperators';

export const WHERE_CLAUSE_CUSTOM_SUFFIX = '-custom';

export const useOptions = (
	key: string,
	keys: BaseAutocompleteData[],
	operator: string,
	searchValue: string,
	isMulti: boolean,
	isValidOperator: boolean,
	isExist: boolean,
	results: string[],
	result: string[],
	whereClauseConfig?: WhereClauseConfig,
): Option[] => {
	const [options, setOptions] = useState<Option[]>([]);
	const operators = useOperators(key, keys);

	const getLabel = useCallback(
		(data: BaseAutocompleteData): Option['label'] =>
			transformStringWithPrefix({
				str: data?.key,
				prefix: data?.type || '',
				condition: !data?.isColumn,
			}),
		[],
	);

	const getOptionsFromKeys = useCallback(
		(items: BaseAutocompleteData[]): Option[] =>
			items?.map((item) => ({
				label: `${getLabel(item)}`,
				value: item.key,
			})),
		[getLabel],
	);

	const getKeyOpValue = useCallback(
		(items: string[]): Option[] =>
			items?.map((item) => ({
				label: `${key} ${operator} ${item}`,
				value: `${key} ${operator} ${item}`,
			})),
		[key, operator],
	);

	const getOptionsWithValidOperator = useCallback(
		(key: string, results: string[], searchValue: string) => {
			const hasAllResults = results.every((value) => result.includes(value));
			const values = getKeyOpValue(results);

			return hasAllResults
				? [
						{
							label: searchValue,
							value: searchValue,
						},
				  ]
				: [
						{
							label: searchValue,
							value: searchValue,
						},
						...values,
				  ];
		},
		[getKeyOpValue, result],
	);

	const getKeyOperatorOptions = useCallback(
		(key: string) => {
			const operatorsOptions = operators?.map((operator) => ({
				value: `${key} ${operator} `,
				label: `${key} ${operator} `,
			}));
			if (whereClauseConfig) {
				return [
					{
						label: `${searchValue} `,
						value: `${searchValue}${WHERE_CLAUSE_CUSTOM_SUFFIX}`,
					},
					...operatorsOptions,
				];
			}
			return operatorsOptions;
		},
		[operators, searchValue, whereClauseConfig],
	);

	useEffect(() => {
		let newOptions: Option[] = [];

		if (!key) {
			newOptions = searchValue
				? [
						{
							label: `${searchValue} `,
							value: `${searchValue} `,
						},
						...getOptionsFromKeys(keys),
				  ]
				: getOptionsFromKeys(keys);
		} else if (key && !operator) {
			newOptions = getKeyOperatorOptions(key);
		} else if (key && operator) {
			if (isMulti) {
				newOptions = results.map((item) => ({
					label: checkCommaInValue(String(item)),
					value: String(item),
				}));
			} else if (isExist) {
				newOptions = [];
			} else if (isValidOperator) {
				newOptions = getOptionsWithValidOperator(key, results, searchValue);
			}
		}
		if (newOptions.length > 0) {
			setOptions(newOptions);
		}
	}, [
		whereClauseConfig,
		getKeyOpValue,
		getOptionsFromKeys,
		isExist,
		isMulti,
		isValidOperator,
		key,
		keys,
		operator,
		operators,
		result,
		results,
		searchValue,
		getKeyOperatorOptions,
		getOptionsWithValidOperator,
	]);

	return useMemo(
		() =>
			options
				.filter(
					(option, index, self) =>
						index ===
							self.findIndex(
								(o) => o.label === option.label && o.value === option.value, // to remove duplicate & empty options from list
							) && option.value !== '',
				)
				.map((option) => {
					const { tagValue } = getTagToken(searchValue);
					if (isMulti) {
						return {
							...option,
							selected: Array.isArray(tagValue)
								? tagValue
										?.filter((i) => i.trim().replace(/^\s+/, '') === option.value)
										?.includes(option.value)
								: String(tagValue).includes(option.value),
						};
					}
					return option;
				}),
		[isMulti, options, searchValue],
	);
};
