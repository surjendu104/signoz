package rules

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.signoz.io/signoz/pkg/query-service/common"
	"go.signoz.io/signoz/pkg/query-service/converter"
	"go.signoz.io/signoz/pkg/query-service/postprocess"

	"go.signoz.io/signoz/pkg/query-service/app/querier"
	querierV2 "go.signoz.io/signoz/pkg/query-service/app/querier/v2"
	"go.signoz.io/signoz/pkg/query-service/app/queryBuilder"
	"go.signoz.io/signoz/pkg/query-service/constants"
	"go.signoz.io/signoz/pkg/query-service/interfaces"
	v3 "go.signoz.io/signoz/pkg/query-service/model/v3"
	"go.signoz.io/signoz/pkg/query-service/utils/labels"
	"go.signoz.io/signoz/pkg/query-service/utils/times"
	"go.signoz.io/signoz/pkg/query-service/utils/timestamp"

	"go.signoz.io/signoz/pkg/query-service/formatter"

	yaml "gopkg.in/yaml.v2"
)

type AnomalyRule struct {
	id            string
	name          string
	source        string
	ruleCondition *RuleCondition
	// evalWindow is the time window used for evaluating the rule
	// i.e each time we lookback from the current time, we look at data for the last
	// evalWindow duration
	evalWindow time.Duration
	// holdDuration is the duration for which the alert waits before firing
	holdDuration time.Duration
	// holds the static set of labels and annotations for the rule
	// these are the same for all alerts created for this rule
	labels      labels.Labels
	annotations labels.Labels

	// preferredChannels is the list of channels to send the alert to
	// if the rule is triggered
	preferredChannels []string
	mtx               sync.Mutex

	// the time it took to evaluate the rule
	evaluationDuration time.Duration
	// the timestamp of the last evaluation
	evaluationTimestamp time.Time

	health RuleHealth

	lastError error

	// map of active alerts
	active map[uint64]*Alert

	// Ever since we introduced the new metrics query builder, the version is "v4"
	// for all the rules
	// if the version is "v3", then we use the old querier
	// if the version is "v4", then we use the new querierV2
	version string
	// temporalityMap is a map of metric name to temporality
	// to avoid fetching temporality for the same metric multiple times
	// querying the v4 table on low cardinal temporality column
	// should be fast but we can still avoid the query if we have the data in memory
	temporalityMap map[string]map[v3.Temporality]bool

	opts AnomalyRuleOpts

	// lastTimestampWithDatapoints is the timestamp of the last datapoint we observed
	// for this rule
	// this is used for missing data alerts
	lastTimestampWithDatapoints time.Time

	// Type of the rule
	// One of ["LOGS_BASED_ALERT", "TRACES_BASED_ALERT", "METRIC_BASED_ALERT", "EXCEPTIONS_BASED_ALERT"]
	typ string

	// querier is used for alerts created before the introduction of new metrics query builder
	querier interfaces.Querier
	// querierV2 is used for alerts created after the introduction of new metrics query builder
	querierV2 interfaces.Querier

	reader    interfaces.Reader
	evalDelay time.Duration
}

type AnomalyRuleOpts struct {
	// sendUnmatched sends observed metric values
	// even if they dont match the rule condition. this is
	// useful in testing the rule
	SendUnmatched bool

	// sendAlways will send alert irresepective of resendDelay
	// or other params
	SendAlways bool

	// EvalDelay is the time to wait for data to be available
	// before evaluating the rule. This is useful in scenarios
	// where data might not be available in the system immediately
	// after the timestamp.
	EvalDelay time.Duration
}

func NewAnomalyRule(
	id string,
	p *PostableRule,
	opts AnomalyRuleOpts,
	featureFlags interfaces.FeatureLookup,
	reader interfaces.Reader,
) (*AnomalyRule, error) {

	zap.L().Info("creating new AnomalyRule", zap.String("id", id), zap.Any("opts", opts))

	if p.RuleCondition == nil {
		return nil, fmt.Errorf("no rule condition")
	} else if !p.RuleCondition.IsValid() {
		return nil, fmt.Errorf("invalid rule condition")
	}

	t := AnomalyRule{
		id:                id,
		name:              p.AlertName,
		source:            p.Source,
		ruleCondition:     p.RuleCondition,
		evalWindow:        time.Duration(p.EvalWindow),
		labels:            labels.FromMap(p.Labels),
		annotations:       labels.FromMap(p.Annotations),
		preferredChannels: p.PreferredChannels,
		health:            HealthUnknown,
		active:            map[uint64]*Alert{},
		opts:              opts,
		typ:               p.AlertType,
		version:           p.Version,
		temporalityMap:    make(map[string]map[v3.Temporality]bool),
		evalDelay:         opts.EvalDelay,
	}

	if int64(t.evalWindow) == 0 {
		t.evalWindow = 5 * time.Minute
	}

	querierOption := querier.QuerierOptions{
		Reader:        reader,
		Cache:         nil,
		KeyGenerator:  queryBuilder.NewKeyGenerator(),
		FeatureLookup: featureFlags,
	}

	querierOptsV2 := querierV2.QuerierOptions{
		Reader:        reader,
		Cache:         nil,
		KeyGenerator:  queryBuilder.NewKeyGenerator(),
		FeatureLookup: featureFlags,
	}

	t.querier = querier.NewQuerier(querierOption)
	t.querierV2 = querierV2.NewQuerier(querierOptsV2)
	t.reader = reader

	zap.L().Info("creating new AnomalyRule", zap.String("name", t.name), zap.String("id", t.id))

	return &t, nil
}

func (r *AnomalyRule) Name() string {
	return r.name
}

func (r *AnomalyRule) ID() string {
	return r.id
}

func (r *AnomalyRule) Condition() *RuleCondition {
	return r.ruleCondition
}

func (r *AnomalyRule) GeneratorURL() string {
	return prepareRuleGeneratorURL(r.ID(), r.source)
}

func (r *AnomalyRule) PreferredChannels() []string {
	return r.preferredChannels
}

// targetVal returns the target value for the rule condition
// when the y-axis and target units are non-empty, it
// converts the target value to the y-axis unit
func (r *AnomalyRule) targetVal() float64 {
	if r.ruleCondition == nil || r.ruleCondition.Target == nil {
		return 0
	}

	// get the converter for the target unit
	unitConverter := converter.FromUnit(converter.Unit(r.ruleCondition.TargetUnit))
	// convert the target value to the y-axis unit
	value := unitConverter.Convert(converter.Value{
		F: *r.ruleCondition.Target,
		U: converter.Unit(r.ruleCondition.TargetUnit),
	}, converter.Unit(r.Unit()))

	return value.F
}

func (r *AnomalyRule) matchType() MatchType {
	if r.ruleCondition == nil {
		return AtleastOnce
	}
	return r.ruleCondition.MatchType
}

func (r *AnomalyRule) compareOp() CompareOp {
	if r.ruleCondition == nil {
		return ValueIsEq
	}
	return r.ruleCondition.CompareOp
}

func (r *AnomalyRule) Type() RuleType {
	return RuleTypeThreshold
}

func (r *AnomalyRule) SetLastError(err error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.lastError = err
}

func (r *AnomalyRule) LastError() error {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.lastError
}

func (r *AnomalyRule) SetHealth(health RuleHealth) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.health = health
}

func (r *AnomalyRule) Health() RuleHealth {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.health
}

// SetEvaluationDuration updates evaluationDuration to the duration it took to evaluate the rule on its last evaluation.
func (r *AnomalyRule) SetEvaluationDuration(dur time.Duration) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.evaluationDuration = dur
}

func (r *AnomalyRule) HoldDuration() time.Duration {
	return r.holdDuration
}

func (r *AnomalyRule) EvalWindow() time.Duration {
	return r.evalWindow
}

// Labels returns the labels of the alerting rule.
func (r *AnomalyRule) Labels() labels.BaseLabels {
	return r.labels
}

// Annotations returns the annotations of the alerting rule.
func (r *AnomalyRule) Annotations() labels.BaseLabels {
	return r.annotations
}

// GetEvaluationDuration returns the time in seconds it took to evaluate the alerting rule.
func (r *AnomalyRule) GetEvaluationDuration() time.Duration {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.evaluationDuration
}

// SetEvaluationTimestamp updates evaluationTimestamp to the timestamp of when the rule was last evaluated.
func (r *AnomalyRule) SetEvaluationTimestamp(ts time.Time) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.evaluationTimestamp = ts
}

// GetEvaluationTimestamp returns the time the evaluation took place.
func (r *AnomalyRule) GetEvaluationTimestamp() time.Time {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.evaluationTimestamp
}

// State returns the maximum state of alert instances for this rule.
// StateFiring > StatePending > StateInactive
func (r *AnomalyRule) State() AlertState {

	maxState := StateInactive
	for _, a := range r.active {
		if a.State > maxState {
			maxState = a.State
		}
	}
	return maxState
}

func (r *AnomalyRule) currentAlerts() []*Alert {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	alerts := make([]*Alert, 0, len(r.active))

	for _, a := range r.active {
		anew := *a
		alerts = append(alerts, &anew)
	}
	return alerts
}

func (r *AnomalyRule) ActiveAlerts() []*Alert {
	var res []*Alert
	for _, a := range r.currentAlerts() {
		if a.ResolvedAt.IsZero() {
			res = append(res, a)
		}
	}
	return res
}

func (r *AnomalyRule) FetchTemporality(ctx context.Context, metricNames []string, ch driver.Conn) (map[string]map[v3.Temporality]bool, error) {

	metricNameToTemporality := make(map[string]map[v3.Temporality]bool)

	query := fmt.Sprintf(`SELECT DISTINCT metric_name, temporality FROM %s.%s WHERE metric_name IN $1`, constants.SIGNOZ_METRIC_DBNAME, constants.SIGNOZ_TIMESERIES_v4_1DAY_TABLENAME)

	rows, err := ch.Query(ctx, query, metricNames)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var metricName, temporality string
		err := rows.Scan(&metricName, &temporality)
		if err != nil {
			return nil, err
		}
		if _, ok := metricNameToTemporality[metricName]; !ok {
			metricNameToTemporality[metricName] = make(map[v3.Temporality]bool)
		}
		metricNameToTemporality[metricName][v3.Temporality(temporality)] = true
	}
	return metricNameToTemporality, nil
}

// populateTemporality same as addTemporality but for v4 and better
func (r *AnomalyRule) populateTemporality(ctx context.Context, qp *v3.QueryRangeParamsV3, ch driver.Conn) error {

	missingTemporality := make([]string, 0)
	metricNameToTemporality := make(map[string]map[v3.Temporality]bool)
	if qp.CompositeQuery != nil && len(qp.CompositeQuery.BuilderQueries) > 0 {
		for _, query := range qp.CompositeQuery.BuilderQueries {
			// if there is no temporality specified in the query but we have it in the map
			// then use the value from the map
			if query.Temporality == "" && r.temporalityMap[query.AggregateAttribute.Key] != nil {
				// We prefer delta if it is available
				if r.temporalityMap[query.AggregateAttribute.Key][v3.Delta] {
					query.Temporality = v3.Delta
				} else if r.temporalityMap[query.AggregateAttribute.Key][v3.Cumulative] {
					query.Temporality = v3.Cumulative
				} else {
					query.Temporality = v3.Unspecified
				}
			}
			// we don't have temporality for this metric
			if query.DataSource == v3.DataSourceMetrics && query.Temporality == "" {
				missingTemporality = append(missingTemporality, query.AggregateAttribute.Key)
			}
			if _, ok := metricNameToTemporality[query.AggregateAttribute.Key]; !ok {
				metricNameToTemporality[query.AggregateAttribute.Key] = make(map[v3.Temporality]bool)
			}
		}
	}

	var nameToTemporality map[string]map[v3.Temporality]bool
	var err error

	if len(missingTemporality) > 0 {
		nameToTemporality, err = r.FetchTemporality(ctx, missingTemporality, ch)
		if err != nil {
			return err
		}
	}

	if qp.CompositeQuery != nil && len(qp.CompositeQuery.BuilderQueries) > 0 {
		for name := range qp.CompositeQuery.BuilderQueries {
			query := qp.CompositeQuery.BuilderQueries[name]
			if query.DataSource == v3.DataSourceMetrics && query.Temporality == "" {
				if nameToTemporality[query.AggregateAttribute.Key][v3.Delta] {
					query.Temporality = v3.Delta
				} else if nameToTemporality[query.AggregateAttribute.Key][v3.Cumulative] {
					query.Temporality = v3.Cumulative
				} else {
					query.Temporality = v3.Unspecified
				}
				r.temporalityMap[query.AggregateAttribute.Key] = nameToTemporality[query.AggregateAttribute.Key]
			}
		}
	}
	return nil
}

// ForEachActiveAlert runs the given function on each alert.
// This should be used when you want to use the actual alerts from the AnomalyRule
// and not on its copy.
// If you want to run on a copy of alerts then don't use this, get the alerts from 'ActiveAlerts()'.
func (r *AnomalyRule) ForEachActiveAlert(f func(*Alert)) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	for _, a := range r.active {
		f(a)
	}
}

func (r *AnomalyRule) SendAlerts(ctx context.Context, ts time.Time, resendDelay time.Duration, interval time.Duration, notifyFunc NotifyFunc) {
	alerts := []*Alert{}
	r.ForEachActiveAlert(func(alert *Alert) {
		if r.opts.SendAlways || alert.needsSending(ts, resendDelay) {
			alert.LastSentAt = ts
			// Allow for two Eval or Alertmanager send failures.
			delta := resendDelay
			if interval > resendDelay {
				delta = interval
			}
			alert.ValidUntil = ts.Add(4 * delta)
			anew := *alert
			alerts = append(alerts, &anew)
		} else {
			zap.L().Debug("skipping send alert due to resend delay", zap.String("rule", r.Name()), zap.Any("alert", alert.Labels))
		}
	})
	notifyFunc(ctx, "", alerts...)
}

func (r *AnomalyRule) Unit() string {
	if r.ruleCondition != nil && r.ruleCondition.CompositeQuery != nil {
		return r.ruleCondition.CompositeQuery.Unit
	}
	return ""
}

type anomalyParams struct {
	CurrentPeriodQuery *v3.QueryRangeParamsV3
	PastPeriodQuery    *v3.QueryRangeParamsV3
	CurrentWeekQuery   *v3.QueryRangeParamsV3
	PastWeekQuery      *v3.QueryRangeParamsV3
}

func (r *AnomalyRule) copyCompositeQuery() *v3.CompositeQuery {
	deepCopyCompositeQuery := *r.ruleCondition.CompositeQuery
	deepCopyCompositeQuery.BuilderQueries = make(map[string]*v3.BuilderQuery)
	for k, v := range r.ruleCondition.CompositeQuery.BuilderQueries {
		query := *v
		deepCopyCompositeQuery.BuilderQueries[k] = &query
	}
	return &deepCopyCompositeQuery
}

func (r *AnomalyRule) prepareQueryRange(ts time.Time) *anomalyParams {

	zap.L().Info("prepareQueryRange", zap.Int64("ts", ts.UnixMilli()), zap.Int64("evalWindow", r.evalWindow.Milliseconds()), zap.Int64("evalDelay", r.evalDelay.Milliseconds()))

	start := ts.Add(-time.Duration(r.evalWindow)).UnixMilli()
	end := ts.UnixMilli()
	if r.evalDelay > 0 {
		start = start - int64(r.evalDelay.Milliseconds())
		end = end - int64(r.evalDelay.Milliseconds())
	}
	// round to minute otherwise we could potentially miss data
	start = start - (start % (60 * 1000))
	end = end - (end % (60 * 1000))

	if r.ruleCondition.CompositeQuery != nil && r.ruleCondition.CompositeQuery.BuilderQueries != nil {
		for _, q := range r.ruleCondition.CompositeQuery.BuilderQueries {
			// If the step interval is less than the minimum allowed step interval, set it to the minimum allowed step interval
			if minStep := common.MinAllowedStepInterval(start, end); q.StepInterval < minStep {
				q.StepInterval = minStep
			}
		}
	}

	if r.ruleCondition.CompositeQuery.PanelType != v3.PanelTypeGraph {
		r.ruleCondition.CompositeQuery.PanelType = v3.PanelTypeGraph
	}

	// default mode
	currentPeriodQuery := &v3.QueryRangeParamsV3{
		Start:          start,
		End:            end,
		Step:           int64(math.Max(float64(common.MinAllowedStepInterval(start, end)), 60)),
		CompositeQuery: r.ruleCondition.CompositeQuery,
		Variables:      make(map[string]interface{}, 0),
		NoCache:        false,
	}

	pastPeriodStart := start - 166*time.Hour.Milliseconds() - 2*time.Hour.Milliseconds()
	pastPeriodEnd := end - 166*time.Hour.Milliseconds() + 2*time.Hour.Milliseconds()

	pastPeriodQuery := &v3.QueryRangeParamsV3{
		Start:          pastPeriodStart,
		End:            pastPeriodEnd,
		Step:           int64(math.Max(float64(common.MinAllowedStepInterval(pastPeriodStart, pastPeriodEnd)), 60)),
		CompositeQuery: r.copyCompositeQuery(),
		Variables:      make(map[string]interface{}, 0),
		NoCache:        false,
	}
	for _, q := range pastPeriodQuery.CompositeQuery.BuilderQueries {
		q.StepInterval = int64(math.Max(float64(common.MinAllowedStepInterval(pastPeriodStart, pastPeriodEnd)), 60))
	}

	// 1w growth trend
	currentWeekStart := start - 7*24*time.Hour.Milliseconds()
	currentWeekEnd := end
	currentWeekQuery := &v3.QueryRangeParamsV3{
		Start:          currentWeekStart,
		End:            currentWeekEnd,
		Step:           int64(math.Max(float64(common.MinAllowedStepInterval(currentWeekStart, currentWeekEnd)), 60)),
		CompositeQuery: r.copyCompositeQuery(),
		Variables:      make(map[string]interface{}, 0),
		NoCache:        false,
	}
	for _, q := range currentWeekQuery.CompositeQuery.BuilderQueries {
		q.StepInterval = int64(math.Max(float64(common.MinAllowedStepInterval(currentWeekStart, currentWeekEnd)), 60))
	}

	pastWeekStart := currentWeekStart - 7*24*time.Hour.Milliseconds()
	pastWeekEnd := currentWeekStart
	pastWeekQuery := &v3.QueryRangeParamsV3{
		Start:          pastWeekStart,
		End:            pastWeekEnd,
		Step:           int64(math.Max(float64(common.MinAllowedStepInterval(pastWeekStart, pastWeekEnd)), 60)),
		CompositeQuery: r.copyCompositeQuery(),
		Variables:      make(map[string]interface{}, 0),
		NoCache:        false,
	}
	for _, q := range pastWeekQuery.CompositeQuery.BuilderQueries {
		q.StepInterval = int64(math.Max(float64(common.MinAllowedStepInterval(pastWeekStart, pastWeekEnd)), 60))
	}

	return &anomalyParams{
		CurrentPeriodQuery: currentPeriodQuery,
		PastPeriodQuery:    pastPeriodQuery,
		CurrentWeekQuery:   currentWeekQuery,
		PastWeekQuery:      pastWeekQuery,
	}
}

// The following function is used to prepare the where clause for the query
// `lbls` contains the key value pairs of the labels from the result of the query
// We iterate over the where clause and replace the labels with the actual values
// There are two cases:
// 1. The label is present in the where clause
// 2. The label is not present in the where clause
//
// Example for case 2:
// Latency by serviceName without any filter
// In this case, for each service with latency > threshold we send a notification
// The expectation will be that clicking on the related traces for service A, will
// take us to the traces page with the filter serviceName=A
// So for all the missing labels in the where clause, we add them as key = value
//
// Example for case 1:
// Severity text IN (WARN, ERROR)
// In this case, the Severity text will appear in the `lbls` if it were part of the group
// by clause, in which case we replace it with the actual value for the notification
// i.e Severity text = WARN
// If the Severity text is not part of the group by clause, then we add it as it is
func (r *AnomalyRule) fetchFilters(selectedQuery string, lbls labels.Labels) []v3.FilterItem {
	var filterItems []v3.FilterItem

	added := make(map[string]struct{})

	if r.ruleCondition.CompositeQuery.QueryType == v3.QueryTypeBuilder &&
		r.ruleCondition.CompositeQuery.BuilderQueries[selectedQuery] != nil &&
		r.ruleCondition.CompositeQuery.BuilderQueries[selectedQuery].Filters != nil {

		for _, item := range r.ruleCondition.CompositeQuery.BuilderQueries[selectedQuery].Filters.Items {
			exists := false
			for _, label := range lbls {
				if item.Key.Key == label.Name {
					// if the label is present in the where clause, replace it with key = value
					filterItems = append(filterItems, v3.FilterItem{
						Key:      item.Key,
						Operator: v3.FilterOperatorEqual,
						Value:    label.Value,
					})
					exists = true
					added[label.Name] = struct{}{}
					break
				}
			}

			if !exists {
				// if the label is not present in the where clause, add it as it is
				filterItems = append(filterItems, item)
			}
		}
	}

	// add the labels which are not present in the where clause
	for _, label := range lbls {
		if _, ok := added[label.Name]; !ok {
			filterItems = append(filterItems, v3.FilterItem{
				Key:      v3.AttributeKey{Key: label.Name},
				Operator: v3.FilterOperatorEqual,
				Value:    label.Value,
			})
		}
	}

	return filterItems
}

func (r *AnomalyRule) prepareLinksToLogs(ts time.Time, lbls labels.Labels) string {
	selectedQuery := r.GetSelectedQuery()

	// TODO(srikanthccv): handle formula queries
	if selectedQuery < "A" || selectedQuery > "Z" {
		return ""
	}

	params := r.prepareQueryRange(ts)
	// Logs list view expects time in milliseconds
	tr := v3.URLShareableTimeRange{
		Start:    params.CurrentPeriodQuery.Start,
		End:      params.CurrentPeriodQuery.End,
		PageSize: 100,
	}

	options := v3.URLShareableOptions{
		MaxLines:      2,
		Format:        "list",
		SelectColumns: []v3.AttributeKey{},
	}

	period, _ := json.Marshal(tr)
	urlEncodedTimeRange := url.QueryEscape(string(period))

	filterItems := r.fetchFilters(selectedQuery, lbls)
	urlData := v3.URLShareableCompositeQuery{
		QueryType: string(v3.QueryTypeBuilder),
		Builder: v3.URLShareableBuilderQuery{
			QueryData: []v3.BuilderQuery{
				{
					DataSource:         v3.DataSourceLogs,
					QueryName:          "A",
					AggregateOperator:  v3.AggregateOperatorNoOp,
					AggregateAttribute: v3.AttributeKey{},
					Filters: &v3.FilterSet{
						Items:    filterItems,
						Operator: "AND",
					},
					Expression:   "A",
					Disabled:     false,
					Having:       []v3.Having{},
					StepInterval: 60,
					OrderBy: []v3.OrderBy{
						{
							ColumnName: "timestamp",
							Order:      "desc",
						},
					},
				},
			},
			QueryFormulas: make([]string, 0),
		},
	}

	data, _ := json.Marshal(urlData)
	compositeQuery := url.QueryEscape(string(data))

	optionsData, _ := json.Marshal(options)
	urlEncodedOptions := url.QueryEscape(string(optionsData))

	return fmt.Sprintf("compositeQuery=%s&timeRange=%s&startTime=%d&endTime=%d&options=%s", compositeQuery, urlEncodedTimeRange, tr.Start, tr.End, urlEncodedOptions)
}

func (r *AnomalyRule) prepareLinksToTraces(ts time.Time, lbls labels.Labels) string {
	selectedQuery := r.GetSelectedQuery()

	// TODO(srikanthccv): handle formula queries
	if selectedQuery < "A" || selectedQuery > "Z" {
		return ""
	}

	params := r.prepareQueryRange(ts)
	// Traces list view expects time in nanoseconds
	tr := v3.URLShareableTimeRange{
		Start:    params.CurrentPeriodQuery.Start * time.Second.Microseconds(),
		End:      params.CurrentPeriodQuery.End * time.Second.Microseconds(),
		PageSize: 100,
	}

	options := v3.URLShareableOptions{
		MaxLines:      2,
		Format:        "list",
		SelectColumns: constants.TracesListViewDefaultSelectedColumns,
	}

	period, _ := json.Marshal(tr)
	urlEncodedTimeRange := url.QueryEscape(string(period))

	filterItems := r.fetchFilters(selectedQuery, lbls)
	urlData := v3.URLShareableCompositeQuery{
		QueryType: string(v3.QueryTypeBuilder),
		Builder: v3.URLShareableBuilderQuery{
			QueryData: []v3.BuilderQuery{
				{
					DataSource:         v3.DataSourceTraces,
					QueryName:          "A",
					AggregateOperator:  v3.AggregateOperatorNoOp,
					AggregateAttribute: v3.AttributeKey{},
					Filters: &v3.FilterSet{
						Items:    filterItems,
						Operator: "AND",
					},
					Expression:   "A",
					Disabled:     false,
					Having:       []v3.Having{},
					StepInterval: 60,
					OrderBy: []v3.OrderBy{
						{
							ColumnName: "timestamp",
							Order:      "desc",
						},
					},
				},
			},
			QueryFormulas: make([]string, 0),
		},
	}

	data, _ := json.Marshal(urlData)
	compositeQuery := url.QueryEscape(string(data))

	optionsData, _ := json.Marshal(options)
	urlEncodedOptions := url.QueryEscape(string(optionsData))

	return fmt.Sprintf("compositeQuery=%s&timeRange=%s&startTime=%d&endTime=%d&options=%s", compositeQuery, urlEncodedTimeRange, tr.Start, tr.End, urlEncodedOptions)
}

func (r *AnomalyRule) hostFromSource() string {
	parsedUrl, err := url.Parse(r.source)
	if err != nil {
		return ""
	}
	if parsedUrl.Port() != "" {
		return fmt.Sprintf("%s://%s:%s", parsedUrl.Scheme, parsedUrl.Hostname(), parsedUrl.Port())
	}
	return fmt.Sprintf("%s://%s", parsedUrl.Scheme, parsedUrl.Hostname())
}

func (r *AnomalyRule) GetSelectedQuery() string {
	if r.ruleCondition != nil {
		if r.ruleCondition.SelectedQuery != "" {
			return r.ruleCondition.SelectedQuery
		}

		queryNames := map[string]struct{}{}

		if r.ruleCondition.CompositeQuery != nil {
			if r.ruleCondition.QueryType() == v3.QueryTypeBuilder {
				for name := range r.ruleCondition.CompositeQuery.BuilderQueries {
					queryNames[name] = struct{}{}
				}
			} else if r.ruleCondition.QueryType() == v3.QueryTypeClickHouseSQL {
				for name := range r.ruleCondition.CompositeQuery.ClickHouseQueries {
					queryNames[name] = struct{}{}
				}
			}
		}

		// The following logic exists for backward compatibility
		// If there is no selected query, then
		// - check if F1 is present, if yes, return F1
		// - else return the query with max ascii value
		// this logic is not really correct. we should be considering
		// whether the query is enabled or not. but this is a temporary
		// fix to support backward compatibility
		if _, ok := queryNames["F1"]; ok {
			return "F1"
		}
		keys := make([]string, 0, len(queryNames))
		for k := range queryNames {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return keys[len(keys)-1]
	}
	// This should never happen
	return ""
}

func (r *AnomalyRule) buildAndRunQuery(ctx context.Context, ts time.Time, ch clickhouse.Conn) (Vector, error) {
	if r.ruleCondition == nil || r.ruleCondition.CompositeQuery == nil {
		r.SetHealth(HealthBad)
		r.SetLastError(fmt.Errorf("no rule condition"))
		return nil, fmt.Errorf("invalid rule condition")
	}

	params := r.prepareQueryRange(ts)
	currErr := r.populateTemporality(ctx, params.CurrentPeriodQuery, ch)
	prevErr := r.populateTemporality(ctx, params.PastPeriodQuery, ch)
	weekErr := r.populateTemporality(ctx, params.CurrentWeekQuery, ch)
	weekPrevErr := r.populateTemporality(ctx, params.PastWeekQuery, ch)
	if currErr != nil || prevErr != nil || weekErr != nil || weekPrevErr != nil {
		r.SetHealth(HealthBad)
		zap.L().Error("failed to set temporality", zap.String("rule", r.Name()), zap.Error(currErr), zap.Error(prevErr), zap.Error(weekErr), zap.Error(weekPrevErr))
		return nil, fmt.Errorf("internal error while setting temporality")
	}

	jsun, _ := json.Marshal(params)
	fmt.Println("params", string(jsun))

	currPeriodResults, _, currPeriodErr := r.querierV2.QueryRange(ctx, params.CurrentPeriodQuery, map[string]v3.AttributeKey{})
	prevPeriodResults, _, prevPeriodErr := r.querierV2.QueryRange(ctx, params.PastPeriodQuery, map[string]v3.AttributeKey{})
	weekResults, _, weekErr := r.querierV2.QueryRange(ctx, params.CurrentWeekQuery, map[string]v3.AttributeKey{})
	weekPrevResults, _, weekPrevErr := r.querierV2.QueryRange(ctx, params.PastWeekQuery, map[string]v3.AttributeKey{})

	if currPeriodErr != nil || prevPeriodErr != nil || weekErr != nil || weekPrevErr != nil {
		zap.L().Error("failed to get alert query result", zap.String("rule", r.Name()), zap.Error(currPeriodErr), zap.Error(prevPeriodErr), zap.Error(weekErr), zap.Error(weekPrevErr))
		r.SetHealth(HealthBad)
		return nil, fmt.Errorf("internal error while querying")
	}

	currPeriodResults, currPostErr := postprocess.PostProcessResult(currPeriodResults, params.CurrentPeriodQuery)
	prevPeriodResults, prevPostErr := postprocess.PostProcessResult(prevPeriodResults, params.PastPeriodQuery)
	weekResults, weekPostErr := postprocess.PostProcessResult(weekResults, params.CurrentWeekQuery)
	weekPrevResults, weekPrevPostErr := postprocess.PostProcessResult(weekPrevResults, params.PastWeekQuery)

	if currPostErr != nil || prevPostErr != nil || weekPostErr != nil || weekPrevPostErr != nil {
		zap.L().Error("failed to postprocess alert query result", zap.String("rule", r.Name()), zap.Error(currPostErr), zap.Error(prevPostErr), zap.Error(weekPostErr), zap.Error(weekPrevPostErr))
		r.SetHealth(HealthBad)
		return nil, fmt.Errorf("internal error while postprocessing")
	}

	selectedQuery := r.GetSelectedQuery()

	var currQueryResult, prevQueryResult, weekQueryResult, weekPrevQueryResult *v3.Result
	for _, res := range currPeriodResults {
		if res.QueryName == selectedQuery {
			currQueryResult = res
			break
		}
	}
	for _, res := range prevPeriodResults {
		if res.QueryName == selectedQuery {
			prevQueryResult = res
			break
		}
	}

	for _, res := range weekResults {
		if res.QueryName == selectedQuery {
			weekQueryResult = res
			break
		}
	}
	for _, res := range weekPrevResults {
		if res.QueryName == selectedQuery {
			weekPrevQueryResult = res
			break
		}
	}

	if currQueryResult != nil && len(currQueryResult.Series) > 0 {
		r.lastTimestampWithDatapoints = time.Now()
	}

	var resultVector Vector

	// if the data is missing for `For` duration then we should send alert
	if r.ruleCondition.AlertOnAbsent && r.lastTimestampWithDatapoints.Add(time.Duration(r.Condition().AbsentFor)*time.Minute).Before(time.Now()) {
		zap.L().Info("no data found for rule condition", zap.String("ruleid", r.ID()))
		lbls := labels.NewBuilder(labels.Labels{})
		if !r.lastTimestampWithDatapoints.IsZero() {
			lbls.Set("lastSeen", r.lastTimestampWithDatapoints.Format(constants.AlertTimeFormat))
		}
		resultVector = append(resultVector, Sample{
			Metric:    lbls.Labels(),
			IsMissing: true,
		})
		return resultVector, nil
	}

	for _, series := range currQueryResult.Series {
		prevSeries := r.getMatchingSeries(prevQueryResult, series)
		weekSeries := r.getMatchingSeries(weekQueryResult, series)
		weekPrevSeries := r.getMatchingSeries(weekPrevQueryResult, series)

		shouldAlert := r.shouldAlert(series, prevSeries, weekSeries, weekPrevSeries)
		if shouldAlert {
			// do something
			fmt.Println("should alert")
		}
	}
	return resultVector, nil
}

func (r *AnomalyRule) getMatchingSeries(queryResult *v3.Result, series *v3.Series) *v3.Series {
	for _, curr := range queryResult.Series {
		currLabels := labels.FromMap(curr.Labels)
		seriesLabels := labels.FromMap(series.Labels)
		if currLabels.Hash() == seriesLabels.Hash() {
			return curr
		}
	}
	return nil
}

func (r *AnomalyRule) Eval(ctx context.Context, ts time.Time, queriers *Queriers) (interface{}, error) {

	prevState := r.State()

	valueFormatter := formatter.FromUnit(r.Unit())
	res, err := r.buildAndRunQuery(ctx, ts, queriers.Ch)

	if err != nil {
		r.SetHealth(HealthBad)
		r.SetLastError(err)
		zap.L().Error("failure in buildAndRunQuery", zap.String("ruleid", r.ID()), zap.Error(err))
		return nil, err
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	resultFPs := map[uint64]struct{}{}
	var alerts = make(map[uint64]*Alert, len(res))

	for _, smpl := range res {
		l := make(map[string]string, len(smpl.Metric))
		for _, lbl := range smpl.Metric {
			l[lbl.Name] = lbl.Value
		}

		value := valueFormatter.Format(smpl.V, r.Unit())
		threshold := valueFormatter.Format(r.targetVal(), r.Unit())
		zap.L().Debug("Alert template data for rule", zap.String("name", r.Name()), zap.String("formatter", valueFormatter.Name()), zap.String("value", value), zap.String("threshold", threshold))

		tmplData := AlertTemplateData(l, value, threshold)
		// Inject some convenience variables that are easier to remember for users
		// who are not used to Go's templating system.
		defs := "{{$labels := .Labels}}{{$value := .Value}}{{$threshold := .Threshold}}"

		// utility function to apply go template on labels and annotations
		expand := func(text string) string {

			tmpl := NewTemplateExpander(
				ctx,
				defs+text,
				"__alert_"+r.Name(),
				tmplData,
				times.Time(timestamp.FromTime(ts)),
				nil,
			)
			result, err := tmpl.Expand()
			if err != nil {
				result = fmt.Sprintf("<error expanding template: %s>", err)
				zap.L().Error("Expanding alert template failed", zap.Error(err), zap.Any("data", tmplData))
			}
			return result
		}

		lb := labels.NewBuilder(smpl.Metric).Del(labels.MetricNameLabel).Del(labels.TemporalityLabel)
		resultLabels := labels.NewBuilder(smpl.MetricOrig).Del(labels.MetricNameLabel).Del(labels.TemporalityLabel).Labels()

		for _, l := range r.labels {
			lb.Set(l.Name, expand(l.Value))
		}

		lb.Set(labels.AlertNameLabel, r.Name())
		lb.Set(labels.AlertRuleIdLabel, r.ID())
		lb.Set(labels.RuleSourceLabel, r.GeneratorURL())

		annotations := make(labels.Labels, 0, len(r.annotations))
		for _, a := range r.annotations {
			annotations = append(annotations, labels.Label{Name: normalizeLabelName(a.Name), Value: expand(a.Value)})
		}
		if smpl.IsMissing {
			lb.Set(labels.AlertNameLabel, "[No data] "+r.Name())
		}

		// Links with timestamps should go in annotations since labels
		// is used alert grouping, and we want to group alerts with the same
		// label set, but different timestamps, together.
		if r.typ == "TRACES_BASED_ALERT" {
			link := r.prepareLinksToTraces(ts, smpl.MetricOrig)
			if link != "" && r.hostFromSource() != "" {
				annotations = append(annotations, labels.Label{Name: "related_traces", Value: fmt.Sprintf("%s/traces-explorer?%s", r.hostFromSource(), link)})
			}
		} else if r.typ == "LOGS_BASED_ALERT" {
			link := r.prepareLinksToLogs(ts, smpl.MetricOrig)
			if link != "" && r.hostFromSource() != "" {
				annotations = append(annotations, labels.Label{Name: "related_logs", Value: fmt.Sprintf("%s/logs/logs-explorer?%s", r.hostFromSource(), link)})
			}
		}

		lbs := lb.Labels()
		h := lbs.Hash()
		resultFPs[h] = struct{}{}

		if _, ok := alerts[h]; ok {
			zap.L().Error("the alert query returns duplicate records", zap.String("ruleid", r.ID()), zap.Any("alert", alerts[h]))
			err = fmt.Errorf("duplicate alert found, vector contains metrics with the same labelset after applying alert labels")
			// We have already acquired the lock above hence using SetHealth and
			// SetLastError will deadlock.
			r.health = HealthBad
			r.lastError = err
			return nil, err
		}

		alerts[h] = &Alert{
			Labels:            lbs,
			QueryResultLables: resultLabels,
			Annotations:       annotations,
			ActiveAt:          ts,
			State:             StatePending,
			Value:             smpl.V,
			GeneratorURL:      r.GeneratorURL(),
			Receivers:         r.preferredChannels,
			Missing:           smpl.IsMissing,
		}
	}

	zap.L().Info("alerts found", zap.String("name", r.Name()), zap.Int("count", len(alerts)))

	// alerts[h] is ready, add or update active list now
	for h, a := range alerts {
		// Check whether we already have alerting state for the identifying label set.
		// Update the last value and annotations if so, create a new alert entry otherwise.
		if alert, ok := r.active[h]; ok && alert.State != StateInactive {

			alert.Value = a.Value
			alert.Annotations = a.Annotations
			alert.Receivers = r.preferredChannels
			continue
		}

		r.active[h] = a

	}

	itemsToAdd := []v3.RuleStateHistory{}

	// Check if any pending alerts should be removed or fire now. Write out alert timeseries.
	for fp, a := range r.active {
		labelsJSON, err := json.Marshal(a.QueryResultLables)
		if err != nil {
			zap.L().Error("error marshaling labels", zap.Error(err), zap.Any("labels", a.Labels))
		}
		if _, ok := resultFPs[fp]; !ok {
			// If the alert was previously firing, keep it around for a given
			// retention time so it is reported as resolved to the AlertManager.
			if a.State == StatePending || (!a.ResolvedAt.IsZero() && ts.Sub(a.ResolvedAt) > resolvedRetention) {
				delete(r.active, fp)
			}
			if a.State != StateInactive {
				a.State = StateInactive
				a.ResolvedAt = ts
				itemsToAdd = append(itemsToAdd, v3.RuleStateHistory{
					RuleID:       r.ID(),
					RuleName:     r.Name(),
					State:        "normal",
					StateChanged: true,
					UnixMilli:    ts.UnixMilli(),
					Labels:       v3.LabelsString(labelsJSON),
					Fingerprint:  a.QueryResultLables.Hash(),
				})
			}
			continue
		}

		if a.State == StatePending && ts.Sub(a.ActiveAt) >= r.holdDuration {
			a.State = StateFiring
			a.FiredAt = ts
			state := "firing"
			if a.Missing {
				state = "no_data"
			}
			itemsToAdd = append(itemsToAdd, v3.RuleStateHistory{
				RuleID:       r.ID(),
				RuleName:     r.Name(),
				State:        state,
				StateChanged: true,
				UnixMilli:    ts.UnixMilli(),
				Labels:       v3.LabelsString(labelsJSON),
				Fingerprint:  a.QueryResultLables.Hash(),
				Value:        a.Value,
			})
		}
	}

	currentState := r.State()

	if currentState != prevState {
		for idx := range itemsToAdd {
			if currentState == StateInactive {
				itemsToAdd[idx].OverallState = "normal"
			} else {
				itemsToAdd[idx].OverallState = currentState.String()
			}
			itemsToAdd[idx].OverallStateChanged = true
		}
	} else {
		for idx := range itemsToAdd {
			itemsToAdd[idx].OverallState = currentState.String()
			itemsToAdd[idx].OverallStateChanged = false
		}
	}

	if len(itemsToAdd) > 0 && r.reader != nil {
		err := r.reader.AddRuleStateHistory(ctx, itemsToAdd)
		if err != nil {
			zap.L().Error("error while inserting rule state history", zap.Error(err), zap.Any("itemsToAdd", itemsToAdd))
		}
	}
	r.health = HealthGood
	r.lastError = err

	return len(r.active), nil
}

func (r *AnomalyRule) String() string {

	ar := PostableRule{
		AlertName:         r.name,
		RuleCondition:     r.ruleCondition,
		EvalWindow:        Duration(r.evalWindow),
		Labels:            r.labels.Map(),
		Annotations:       r.annotations.Map(),
		PreferredChannels: r.preferredChannels,
	}

	byt, err := yaml.Marshal(ar)
	if err != nil {
		return fmt.Sprintf("error marshaling alerting rule: %s", err.Error())
	}

	return string(byt)
}

func (r *AnomalyRule) getAvg(series *v3.Series) float64 {
	var sum float64
	for _, smpl := range series.Points {
		sum += smpl.Value
	}
	return sum / float64(len(series.Points))
}

func (r *AnomalyRule) getStdDev(series *v3.Series) float64 {
	avg := r.getAvg(series)
	var sum float64
	for _, smpl := range series.Points {
		sum += math.Pow(smpl.Value-avg, 2)
	}
	return math.Sqrt(sum / float64(len(series.Points)))
}

func (r *AnomalyRule) getExpectedValue(series, prevSeries, weekSeries, weekPrevSeries *v3.Series) float64 {
	prevSeriesAvg := r.getAvg(prevSeries)
	weekSeriesAvg := r.getAvg(weekSeries)
	weekPrevSeriesAvg := r.getAvg(weekPrevSeries)
	return prevSeriesAvg + weekSeriesAvg - weekPrevSeriesAvg
}

func (r *AnomalyRule) getScore(series, prevSeries, weekSeries, weekPrevSeries *v3.Series, value float64) float64 {
	expectedValue := r.getExpectedValue(series, prevSeries, weekSeries, weekPrevSeries)
	return (value - expectedValue) / r.getStdDev(weekSeries)
}

func (r *AnomalyRule) shouldAlert(series, prevSeries, weekSeries, weekPrevSeries *v3.Series) bool {
	if series == nil || prevSeries == nil || weekSeries == nil || weekPrevSeries == nil {
		return false
	}

	var shouldAlert bool
	var lbls labels.Labels
	var lblsNormalized labels.Labels

	for name, value := range series.Labels {
		lbls = append(lbls, labels.Label{Name: name, Value: value})
		lblsNormalized = append(lblsNormalized, labels.Label{Name: normalizeLabelName(name), Value: value})
	}

	series.Points = removeGroupinSetPoints(*series)

	// nothing to evaluate
	if len(series.Points) == 0 {
		return false
	}

	switch r.matchType() {
	case AtleastOnce:
		// If any sample matches the condition, the rule is firing.
		if r.compareOp() == ValueIsAbove {
			for _, smpl := range series.Points {
				score := r.getScore(series, prevSeries, weekSeries, weekPrevSeries, smpl.Value)
				if score > r.targetVal() {
					shouldAlert = true
					break
				}
			}
		} else if r.compareOp() == ValueIsBelow {
			for _, smpl := range series.Points {
				score := r.getScore(series, prevSeries, weekSeries, weekPrevSeries, smpl.Value)
				if score < r.targetVal() {
					shouldAlert = true
					break
				}
			}
		} else if r.compareOp() == ValueIsEq {
			for _, smpl := range series.Points {
				score := r.getScore(series, prevSeries, weekSeries, weekPrevSeries, smpl.Value)
				if score == r.targetVal() {
					shouldAlert = true
					break
				}
			}
		} else if r.compareOp() == ValueIsNotEq {
			for _, smpl := range series.Points {
				score := r.getScore(series, prevSeries, weekSeries, weekPrevSeries, smpl.Value)
				if score != r.targetVal() {
					shouldAlert = true
					break
				}
			}
		}
	case AllTheTimes:
		// If all samples match the condition, the rule is firing.
		shouldAlert = true
		if r.compareOp() == ValueIsAbove {
			for _, smpl := range series.Points {
				score := r.getScore(series, prevSeries, weekSeries, weekPrevSeries, smpl.Value)
				if score <= r.targetVal() {
					shouldAlert = false
					break
				}
			}
		} else if r.compareOp() == ValueIsBelow {
			for _, smpl := range series.Points {
				score := r.getScore(series, prevSeries, weekSeries, weekPrevSeries, smpl.Value)
				if score >= r.targetVal() {
					shouldAlert = false
					break
				}
			}
		} else if r.compareOp() == ValueIsEq {
			for _, smpl := range series.Points {
				score := r.getScore(series, prevSeries, weekSeries, weekPrevSeries, smpl.Value)
				if score != r.targetVal() {
					shouldAlert = false
					break
				}
			}
		} else if r.compareOp() == ValueIsNotEq {
			for _, smpl := range series.Points {
				score := r.getScore(series, prevSeries, weekSeries, weekPrevSeries, smpl.Value)
				if score == r.targetVal() {
					shouldAlert = false
					break
				}
			}
		}
	case OnAverage:
		// If the average of all samples matches the condition, the rule is firing.
		var sum, count float64
		for _, smpl := range series.Points {
			if math.IsNaN(smpl.Value) || math.IsInf(smpl.Value, 0) {
				continue
			}
			sum += r.getScore(series, prevSeries, weekSeries, weekPrevSeries, smpl.Value)
			count++
		}
		avg := sum / count
		if r.compareOp() == ValueIsAbove {
			if avg > r.targetVal() {
				shouldAlert = true
			}
		} else if r.compareOp() == ValueIsBelow {
			if avg < r.targetVal() {
				shouldAlert = true
			}
		} else if r.compareOp() == ValueIsEq {
			if avg == r.targetVal() {
				shouldAlert = true
			}
		} else if r.compareOp() == ValueIsNotEq {
			if avg != r.targetVal() {
				shouldAlert = true
			}
		}
	case InTotal:
		// If the sum of all samples matches the condition, the rule is firing.
		var sum float64

		for _, smpl := range series.Points {
			if math.IsNaN(smpl.Value) || math.IsInf(smpl.Value, 0) {
				continue
			}
			sum += r.getScore(series, prevSeries, weekSeries, weekPrevSeries, smpl.Value)
		}
		if r.compareOp() == ValueIsAbove {
			if sum > r.targetVal() {
				shouldAlert = true
			}
		} else if r.compareOp() == ValueIsBelow {
			if sum < r.targetVal() {
				shouldAlert = true
			}
		} else if r.compareOp() == ValueIsEq {
			if sum == r.targetVal() {
				shouldAlert = true
			}
		} else if r.compareOp() == ValueIsNotEq {
			if sum != r.targetVal() {
				shouldAlert = true
			}
		}
	}
	return shouldAlert
}