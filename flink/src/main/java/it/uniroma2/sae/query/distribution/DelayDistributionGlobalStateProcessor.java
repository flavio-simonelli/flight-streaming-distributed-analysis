package it.uniroma2.sae.query.distribution;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.io.Serial;

/**
 * Tracks historical state since the beginning of the dataset for DelayDistributionQuery.
 * Emits results at daily intervals.
 */
public class DelayDistributionGlobalStateProcessor 
        extends KeyedProcessFunction<Tuple2<String, Integer>, it.uniroma2.sae.model.FlightRecord, DelayDistributionResult> {
    @Serial
    private static final long serialVersionUID = 1L;

    private transient ValueState<DelayDistributionAccumulator> state;

    @Override
    public void open(OpenContext context) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("global-dist-accum", DelayDistributionAccumulator.class));
    }

    @Override
    public void processElement(it.uniroma2.sae.model.FlightRecord value, Context ctx, Collector<DelayDistributionResult> out) throws Exception {
        if (value.isCancelled() || value.isDiverted()) {
            return;
        }

        DelayDistributionAccumulator acc = state.value();
        if (acc == null) {
            acc = new DelayDistributionAccumulator();
        }

        acc.add(value.getDepDelay());
        state.update(acc);

        long timestamp = ctx.timestamp();
        // Emit results daily (every 24 logic event-time hours)
        long nextDay = ((timestamp / 86400000L) + 1) * 86400000L;
        ctx.timerService().registerEventTimeTimer(nextDay);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DelayDistributionResult> out) throws Exception {
        DelayDistributionAccumulator acc = state.value();
        if (acc != null) {
            Tuple2<String, Integer> key = ctx.getCurrentKey();
            out.collect(new DelayDistributionResult(
                    0L,
                    timestamp,
                    "global",
                    key.f0, // airline
                    key.f1, // hour
                    acc.getCount(),
                    acc.getMin(),
                    acc.getPercentile(0.25),
                    acc.getPercentile(0.50),
                    acc.getPercentile(0.75),
                    acc.getPercentile(0.90),
                    acc.getMax()
            ));
        }
    }
}
