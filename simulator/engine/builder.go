package engine

import (
	"simulator/config"
	"simulator/engine/loader"
	"simulator/engine/scheduler"
	"simulator/engine/waiter"
	"simulator/output"
)

// Builder constructs an Engine by assembling its dependencies.
type Builder struct {
	cfg       *config.Config
	sink      output.Sink
	loader    loader.Loader
	scheduler scheduler.Scheduler
	waiter    waiter.Waiter
}

// NewBuilder initialises a Builder with defaults from cfg.
func NewBuilder(cfg *config.Config, sink output.Sink) *Builder {
	// When out-of-order replay is enabled, wrap the in-order scheduler so the
	// base ordering logic remains the default behavior.
	var sched scheduler.Scheduler = &scheduler.InOrderScheduler{}
	if cfg.OutOfOrderFactor > 0 && cfg.OutOfOrderMaxDelayMinutes > 0 {
		sched = scheduler.NewOutOfOrderScheduler(&scheduler.InOrderScheduler{}, cfg.OutOfOrderFactor, cfg.OutOfOrderMaxDelayMinutes)
	}

	return &Builder{
		cfg:       cfg,
		sink:      sink,
		loader:    loader.NewCsvLoader(cfg),
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
