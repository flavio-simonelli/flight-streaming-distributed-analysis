package it.uniroma2.sae.query.q2;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serial;

/**
 * Manages historic state accumulation updated dynamically on a global scale.
 * Schedules strategic evaluation timers targeting hourly reporting intervals.
 */
public class Q2GlobalStateProcessor extends KeyedProcessFunction<Integer, FlightRecord, Q2AirportResult> {

    @Serial
    private static final long serialVersionUID = 1L;

    private transient ValueState<Q2Accumulator> state;

    @Override
    public void open(OpenContext context) {
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

        long timestamp = ctx.timestamp();
        long nextHour = ((timestamp / 3600000) + 1) * 3600000;
        ctx.timerService().registerEventTimeTimer(nextHour);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Q2AirportResult> out) throws Exception {
        Q2Accumulator acc = state.value();
        if (acc != null) {
            out.collect(new Q2AirportResult(
                    0L,
                    timestamp,
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