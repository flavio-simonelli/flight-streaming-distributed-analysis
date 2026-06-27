package engine

import (
	"simulator/config"
	"simulator/engine/loader"
	"simulator/engine/scheduler"
	"simulator/engine/waiter"
	"simulator/output"
)

// Builder constructs an Engine by progressively wiring its component dependencies.
// It applies the GoF Builder pattern, separating the construction of a complex object
// from its representation and allowing the caller to override individual components
// (e.g. replace the Loader for testing) without changing the Engine itself.
//
// Default components are inferred from the provided Config:
//   - Loader:    ParquetLoader reading from Config.InputParquetPath
//   - Scheduler: InOrderScheduler, or OutOfOrderScheduler if out-of-order is configured
//   - Waiter:    HybridWaiter with Config.SpinThresholdMs
type Builder struct {
	cfg       *config.Config
	sink      output.Sink
	loader    loader.Loader
	scheduler scheduler.Scheduler
	waiter    waiter.Waiter
}

// NewBuilder initialises a Builder with sensible defaults derived from cfg.
func NewBuilder(cfg *config.Config, sink output.Sink) *Builder {
	var sched scheduler.Scheduler = &scheduler.InOrderScheduler{}
	if cfg.OutOfOrderFactor > 0 && cfg.OutOfOrderMaxDelayMinutes > 0 {
		sched = scheduler.NewOutOfOrderScheduler(&scheduler.InOrderScheduler{}, cfg.OutOfOrderFactor, cfg.OutOfOrderMaxDelayMinutes)
	}

	var l loader.Loader
	switch cfg.DataSourceType {
	case "remote":
		l = loader.NewCsvLoader(cfg)
	default:
		l = loader.NewParquetLoader(cfg.InputParquetPath, cfg.ParquetReaderConcurrency)
	}

	return &Builder{
		cfg:       cfg,
		sink:      sink,
		loader:    l,
		scheduler: sched,
		waiter:    waiter.NewHybridWaiter(cfg.SpinThresholdMs),
	}
}

// WithLoader overrides the default Loader.
func (b *Builder) WithLoader(l loader.Loader) *Builder {
	b.loader = l
	return b
}

// WithScheduler overrides the default Scheduler.
func (b *Builder) WithScheduler(s scheduler.Scheduler) *Builder {
	b.scheduler = s
	return b
}

// WithWaiter overrides the default Waiter.
func (b *Builder) WithWaiter(w waiter.Waiter) *Builder {
	b.waiter = w
	return b
}

// Build assembles and returns the configured Engine.
func (b *Builder) Build() *Engine {
	return &Engine{
		config:    b.cfg,
		sink:      b.sink,
		loader:    b.loader,
		scheduler: b.scheduler,
		waiter:    b.waiter,
	}
}
