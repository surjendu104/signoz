package rules

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"
	v3 "go.signoz.io/signoz/pkg/query-service/model/v3"
	"go.signoz.io/signoz/pkg/query-service/utils/labels"
)

var (
	ErrInvalidCompositeQuery = errors.New("invalid composite query")
	ErrNilCompositeQuery     = errors.New("composite query is nil")
	ErrNilTarget             = errors.New("target is nil")
	ErrInvalidCompareOp      = errors.New("invalid compare op")
	ErrNoPromQLQuery         = errors.New("no promql query")
	ErrInvalidPromQLQuery    = errors.New("invalid promql query")
)

// this file contains common structs and methods used by
// rule engine

const (
	// how long before re-sending the alert
	resolvedRetention = 15 * time.Minute

	testAlertPostFix = "_TEST_ALERT"
)

type RuleType string

const (
	RuleTypeThreshold = "threshold_rule"
	RuleTypeProm      = "promql_rule"
)

type RuleHealth string

const (
	HealthUnknown RuleHealth = "unknown"
	HealthGood    RuleHealth = "ok"
	HealthBad     RuleHealth = "err"
)

// AlertState denotes the state of an active alert.
type AlertState int

const (
	StateInactive AlertState = iota
	StatePending
	StateFiring
	StateDisabled
)

func (s AlertState) String() string {
	switch s {
	case StateInactive:
		return "inactive"
	case StatePending:
		return "pending"
	case StateFiring:
		return "firing"
	case StateDisabled:
		return "disabled"
	}
	panic(errors.Errorf("unknown alert state: %d", s))
}

func (s AlertState) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s *AlertState) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case string:
		switch value {
		case "inactive":
			*s = StateInactive
		case "pending":
			*s = StatePending
		case "firing":
			*s = StateFiring
		case "disabled":
			*s = StateDisabled
		default:
			return errors.New("invalid alert state")
		}
		return nil
	default:
		return errors.New("invalid alert state")
	}
}

type Alert struct {
	State AlertState

	// Labels are used to differentiate an alert from all other alerts
	// This is used for grouping and deduplication
	Labels labels.BaseLabels
	// Annotations are used to provide additional information about an alert
	// This is used for displaying additional information in the UI
	Annotations labels.BaseLabels

	// QueryResultLables are used to store the labels of the query result
	// This is used for storing the labels of the query result
	QueryResultLables labels.BaseLabels

	// GeneratorURL is the URL to the rule that generated this alert
	GeneratorURL string

	// Receivers is a list of preferred receivers, e.g. slack
	Receivers []string

	// Value is the value of the alert
	Value float64

	// ActiveAt is the time the alert was first active
	ActiveAt time.Time

	// FiredAt is the time the alert was first fired
	FiredAt time.Time

	// ResolvedAt is the time the alert was resolved
	ResolvedAt time.Time

	// LastSentAt is the time the alert was last sent
	LastSentAt time.Time

	// ValidUntil is the time the alert will be valid until
	ValidUntil time.Time

	Missing bool
}

// needsSending checks if the alert needs to be sent again
// based on the current time and the last sent time
func (a *Alert) needsSending(ts time.Time, resendDelay time.Duration) bool {
	if a.State == StatePending {
		return false
	}

	// if an alert has been resolved since the last send, resend it
	if a.ResolvedAt.After(a.LastSentAt) {
		return true
	}

	return a.LastSentAt.Add(resendDelay).Before(ts)
}

type NamedAlert struct {
	Name string
	*Alert
}

type CompareOp string

const (
	CompareOpNone CompareOp = "0"
	ValueIsAbove  CompareOp = "1"
	ValueIsBelow  CompareOp = "2"
	ValueIsEq     CompareOp = "3"
	ValueIsNotEq  CompareOp = "4"
)

func ResolveCompareOp(cop CompareOp) string {
	switch cop {
	case ValueIsAbove:
		return ">"
	case ValueIsBelow:
		return "<"
	case ValueIsEq:
		return "=="
	case ValueIsNotEq:
		return "!="
	}
	return ""
}

type MatchType string

const (
	MatchTypeNone MatchType = "0"
	AtleastOnce   MatchType = "1"
	AllTheTimes   MatchType = "2"
	OnAverage     MatchType = "3"
	InTotal       MatchType = "4"
)

type RuleCondition struct {
	// CompositeQuery is the composite query for the rule condition
	// This is non-nil when alert is created using UI
	CompositeQuery *v3.CompositeQuery `json:"compositeQuery,omitempty" yaml:"compositeQuery,omitempty"`
	// CompareOp is the comparison operator for the rule condition
	CompareOp CompareOp `yaml:"op,omitempty" json:"op,omitempty"`
	// Target is the target value for the rule condition
	// This is pointer for legacy reasons, because promql query is expressive enough to express any condition
	Target *float64 `yaml:"target,omitempty" json:"target,omitempty"`
	// AlertOnAbsent is a flag to trigger an alert if the target is absent
	AlertOnAbsent bool `yaml:"alertOnAbsent,omitempty" json:"alertOnAbsent,omitempty"`
	// AbsentFor is the duration for which the target must be absent to trigger an alert
	AbsentFor uint64 `yaml:"absentFor,omitempty" json:"absentFor,omitempty"`
	// MatchType is the type of match to be used for the rule condition
	MatchType MatchType `json:"matchType,omitempty"`
	// TargetUnit is the unit of the target value
	TargetUnit string `json:"targetUnit,omitempty"`
	// SelectedQuery is the name of the query who's result is used for this rule condition
	SelectedQuery string `json:"selectedQueryName,omitempty"`
}

func (rc *RuleCondition) Validate() error {

	if rc.CompositeQuery == nil {
		return ErrNilCompositeQuery
	}

	rc.CompositeQuery.Sanitize()

	if err := rc.CompositeQuery.Validate(); err != nil && !errors.Is(err, v3.ErrInvalidPanelType) {
		return errors.Wrap(err, "invalid composite query")
	}

	if rc.QueryType() == v3.QueryTypeBuilder {
		if rc.Target == nil {
			return ErrNilTarget
		}
		if rc.CompareOp == "" {
			return ErrInvalidCompareOp
		}
	}
	if rc.QueryType() == v3.QueryTypePromQL {
		if len(rc.CompositeQuery.PromQueries) == 0 {
			return ErrNoPromQLQuery
		}
	}
	return nil
}

// QueryType is a short hand method to get query type
func (rc *RuleCondition) QueryType() v3.QueryType {
	if rc.CompositeQuery != nil {
		return rc.CompositeQuery.QueryType
	}
	return v3.QueryTypeUnknown
}

// String is useful in printing rule condition in logs
func (rc *RuleCondition) String() string {
	if rc == nil {
		return ""
	}
	data, _ := json.Marshal(*rc)
	return string(data)
}

type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*d = Duration(time.Duration(value))
		return nil
	case string:
		tmp, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		*d = Duration(tmp)

		return nil
	default:
		return errors.New("invalid duration")
	}
}

// prepareRuleGeneratorURL creates an appropriate url
// for the rule. the URL is sent in slack messages as well as
// to other systems and allows backtracking to the rule definition
// from the third party systems.
func prepareRuleGeneratorURL(ruleId string, source string) string {
	if source == "" {
		return source
	}

	// check if source is a valid url
	parsedSource, err := url.Parse(source)
	if err != nil {
		return ""
	}
	// since we capture window.location when a new rule is created
	// we end up with rulesource host:port/alerts/new. in this case
	// we want to replace new with rule id parameter

	hasNew := strings.LastIndex(source, "new")
	if hasNew > -1 {
		ruleURL := fmt.Sprintf("%sedit?ruleId=%s", source[0:hasNew], ruleId)
		return ruleURL
	}

	// The source contains the encoded query, start and end time
	// and other parameters. We don't want to include them in the generator URL
	// mainly to keep the URL short and lower the alert body contents
	// The generator URL with /alerts/edit?ruleId= is enough
	if parsedSource.Port() != "" {
		return fmt.Sprintf("%s://%s:%s/alerts/edit?ruleId=%s", parsedSource.Scheme, parsedSource.Hostname(), parsedSource.Port(), ruleId)
	}
	return fmt.Sprintf("%s://%s/alerts/edit?ruleId=%s", parsedSource.Scheme, parsedSource.Hostname(), ruleId)
}
