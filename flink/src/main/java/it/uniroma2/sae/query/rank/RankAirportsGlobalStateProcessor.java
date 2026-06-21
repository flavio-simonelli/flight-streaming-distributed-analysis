package it.uniroma2.sae.query.rank;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.io.Serial;

/**
 * Tracks historical state since the beginning of the dataset for RankAirportsQuery.
 * Triggers every hour to emit current statistics.
 */
public class RankAirportsGlobalStateProcessor 
        extends KeyedProcessFunction<Integer, FlightRecord, RankAirportsResult> {
    @Serial
    private static final long serialVersionUID = 1L;

    private transient ValueState<RankAirportsAccumulator> state;

    @Override
    public void open(OpenContext context) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("global-accum", RankAirportsAccumulator.class));
    }

    @Override
    public void processElement(FlightRecord value, Context ctx, Collector<RankAirportsResult> out) throws Exception {
        if (value.isCancelled() || value.isDiverted()) {
            return;
        }

        RankAirportsAccumulator acc = state.value();
        if (acc == null) {
            acc = new RankAirportsAccumulator();
        }

        acc.add(value.getAirline(), value.getDest(), value.getDepDelay());
        state.update(acc);

        long timestamp = ctx.timestamp();
        long nextHour = ((timestamp / 3600000) + 1) * 3600000;
        ctx.timerService().registerEventTimeTimer(nextHour);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RankAirportsResult> out) throws Exception {
        RankAirportsAccumulator acc = state.value();
        if (acc != null) {
            out.collect(new RankAirportsResult(
                    0L,
                    timestamp,
                    "global",
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
