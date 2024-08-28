package rules

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.signoz.io/signoz/pkg/query-service/app/clickhouseReader"
	"go.signoz.io/signoz/pkg/query-service/featureManager"
	v3 "go.signoz.io/signoz/pkg/query-service/model/v3"

	cmock "github.com/srikanthccv/ClickHouse-go-mock"
)

func TestAnomalyRuleNoData(t *testing.T) {
	postableRule := PostableRule{
		AlertName:  "Units test",
		AlertType:  "METRIC_BASED_ALERT",
		RuleType:   RuleTypeThreshold,
		EvalWindow: Duration(5 * time.Minute),
		Frequency:  Duration(1 * time.Minute),
		RuleCondition: &RuleCondition{
			CompositeQuery: &v3.CompositeQuery{
				QueryType: v3.QueryTypeBuilder,
				BuilderQueries: map[string]*v3.BuilderQuery{
					"A": {
						QueryName:    "A",
						StepInterval: 60,
						AggregateAttribute: v3.AttributeKey{
							Key: "signoz_calls_total",
						},
						AggregateOperator: v3.AggregateOperatorSumRate,
						DataSource:        v3.DataSourceMetrics,
						Expression:        "A",
						TimeAggregation:   v3.TimeAggregationRate,
						SpaceAggregation:  v3.SpaceAggregationSum,
					},
				},
			},
			AlertOnAbsent: true,
		},
	}
	fm := featureManager.StartManager()
	mock, err := cmock.NewClickHouseWithQueryMatcher(nil, &queryMatcherAny{})
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	cols := make([]cmock.ColumnType, 0)
	cols = append(cols, cmock.ColumnType{Name: "value", Type: "Float64"})
	cols = append(cols, cmock.ColumnType{Name: "attr", Type: "String"})
	cols = append(cols, cmock.ColumnType{Name: "timestamp", Type: "String"})

	cases := []struct {
		values       [][]interface{}
		expectNoData bool
	}{
		{
			values:       [][]interface{}{},
			expectNoData: true,
		},
	}

	for _, c := range cases {
		rows := cmock.NewRows(cols, c.values)

		// We are testing the eval logic after the query is run
		// so we don't care about the query string here
		queryString := "SELECT any"
		mock.
			ExpectQuery(queryString).
			WillReturnRows(rows)
		var target float64 = 0
		postableRule.RuleCondition.CompareOp = ValueIsEq
		postableRule.RuleCondition.MatchType = AtleastOnce
		postableRule.RuleCondition.Target = &target
		postableRule.Annotations = map[string]string{
			"description": "This alert is fired when the defined metric (current value: {{$value}}) crosses the threshold ({{$threshold}})",
			"summary":     "The rule threshold is set to {{$threshold}}, and the observed metric value is {{$value}}",
		}

		options := clickhouseReader.NewOptions("", 0, 0, 0, "", "archiveNamespace")
		reader := clickhouseReader.NewReaderFromClickhouseConnection(mock, options, nil, "", fm, "")

		rule, err := NewAnomalyRule("69", &postableRule, AnomalyRuleOpts{}, fm, reader)
		rule.temporalityMap = map[string]map[v3.Temporality]bool{
			"signoz_calls_total": {
				v3.Delta: true,
			},
		}
		if err != nil {
			assert.NoError(t, err)
		}

		queriers := Queriers{
			Ch: mock,
		}

		_, err = rule.Eval(context.Background(), time.Now(), &queriers)
		if err != nil {
			assert.NoError(t, err)
		}
	}
}
