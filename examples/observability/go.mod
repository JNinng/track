// 独立模块：把 prometheus 依赖树隔离在示例内，
// 引擎根模块保持零第三方依赖（与 observ 自身 adapters 独立成模块同理）。
//
// 运行：cd examples/observability && go run .
module github.com/jninng/track/examples/observability

go 1.26.2

require (
	github.com/jninng/observ v0.1.0
	github.com/jninng/observ/adapters/prom v0.1.0
	github.com/jninng/track v0.0.0
	github.com/prometheus/client_golang v1.19.1
	github.com/prometheus/common v0.48.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	golang.org/x/sys v0.17.0 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
)

replace github.com/jninng/track => ../..
