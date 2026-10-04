package metrics

import (
	"context"
	"testing"

	"github.com/hawxxx/kaflux/backend/internal/model"
)

func TestSimulatorIsExplicitAndUsesBackendInventory(t *testing.T) {
	source := DemoSource("demo", func(context.Context) (model.Snapshot, error) {
		return model.Snapshot{Brokers: []model.Broker{{ID: 1}, {ID: 2}}, Topics: []model.Topic{{Name: "orders"}}}, nil
	})
	if g, err := NewGateway([]SourceConfig{source}, Options{}); err == nil {
		g.Close()
		t.Fatal("simulated metrics accepted in production")
	}
	g, err := NewGateway([]SourceConfig{source}, Options{Demo: true})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	defs := DefaultCatalog()
	expression := ""
	for _, d := range defs {
		if d.ID == "brokers" {
			expression, _ = Expand(d.Expression, "", "15m")
		}
	}
	if expression == "" {
		t.Fatal("missing broker catalog metric")
	}
	result, err := g.Wait(context.Background(), Query{Source: source.ID, Expression: expression, Start: 100, End: 200, Step: 10})
	if err != nil {
		t.Fatal(err)
	}
	if result.Series[0].Points[0].Value != 2 || result.Series[0].Labels["source"] != "simulator" {
		t.Fatalf("simulator not explicit or inventory inconsistent %+v", result)
	}
}
