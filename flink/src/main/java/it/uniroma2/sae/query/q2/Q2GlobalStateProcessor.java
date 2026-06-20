package it.uniroma2.sae.query.q2;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Q2GlobalStateProcessor extends KeyedProcessFunction<Integer, FlightRecord, Q2AirportResult> {
    private static final long serialVersionUID = 1L;

    private transient ValueState<Q2Accumulator> state;

    @Override
    public void open(Configuration parameters) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("global-accum", Q2Accumulator.class));
    }

    @Override
    public void processElement(FlightRecord value, Context ctx, Collector<Q2AirportResult> out) throws Exception {
        if (value.isCancelled() || value.isDiverted()) {
            return;
        }

        Q2Accumulator acc = state.value();
        if (acc == null) {
            acc = new Q2Accumulator();
        }

        acc.add(value.getAirline(), value.getDest(), value.getDepDelay());
        state.update(acc);

        // Register a timer at the end of the current hour to emit the cumulative state
        long timestamp = ctx.timestamp();
        long nextHour = ((timestamp / 3600000) + 1) * 3600000;
        ctx.timerService().registerEventTimeTimer(nextHour);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Q2AirportResult> out) throws Exception {
        Q2Accumulator acc = state.value();
        if (acc != null) {
            // Emits the global cumulative result up to this hour
            out.collect(new Q2AirportResult(
                    0L, // Global start is 0
                    timestamp, // Current hour boundary
                    ctx.getCurrentKey(),
                    acc.getNumFlights(),
                    acc.getSevereDelays(),
                    acc.getNumFlights() > 0 ? acc.getSumDepDelay() / acc.getNumFlights() : 0.0,
                    acc.getMaxDepDelay(),
                    acc.getDelayedFlights()
            ));
        }
    }
}
