package it.uniroma2.sae.query.rank;

import it.uniroma2.sae.metrics.ProcessingLatencyTracker;
import org.apache.flink.api.common.functions.OpenContext;
import it.uniroma2.sae.utils.MathUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serial;
import java.util.Iterator;

/**
 * Window processor for RankAirportsQuery.
 * Emits RankAirportsResult with specific window type label.
 */
public class RankAirportsWindowProcessor 
        extends ProcessWindowFunction<RankAirportsAccumulator, RankAirportsResult, Integer, TimeWindow> {
    @Serial
    private static final long serialVersionUID = 1L;

    private final String windowType;
    private transient ProcessingLatencyTracker latencyTracker;

    @Override
    public void open(OpenContext context) throws Exception {
        super.open(context);
        this.latencyTracker = new ProcessingLatencyTracker("q2_window_" + windowType);
        this.latencyTracker.register(getRuntimeContext().getMetricGroup());
    }

    public RankAirportsWindowProcessor(String windowType) {
        this.windowType = windowType;
    }

    @Override
    public void process(
            Integer originAirportId,
            Context ctx,
            Iterable<RankAirportsAccumulator> elements,
            Collector<RankAirportsResult> out) throws Exception {

        long start = System.currentTimeMillis();
        try {
            processInternal(originAirportId, ctx, elements, out);
        } finally {
            long duration = System.currentTimeMillis() - start;
            if (latencyTracker != null) {
                latencyTracker.updateOperator(duration);
                Iterator<RankAirportsAccumulator> iterator = elements.iterator();
                if (iterator.hasNext()) {
                    RankAirportsAccumulator acc = iterator.next();
                    latencyTracker.updateE2E(acc.getMaxSystemIngestionTime());
                }
            }
        }
    }

    private void processInternal(
            Integer originAirportId,
            Context ctx,
            Iterable<RankAirportsAccumulator> elements,
            Collector<RankAirportsResult> out) {

        RankAirportsAccumulator acc = elements.iterator().next();

        if (acc.getNumFlights() < 30) {
            return;
        }

        long windowStartRaw = ctx.window().getStart();
        long windowEndRaw   = ctx.window().getEnd();

        double mean = MathUtils.safeDivideRounded(acc.getSumDepDelay(), acc.getNumFlights());

        RankAirportsResult result = new RankAirportsResult(
                windowStartRaw, windowEndRaw, windowType,
                originAirportId,
                acc.getNumFlights(), acc.getSevereDelays(),
                mean,
                acc.getMaxDepDelay(), acc.getDelayedFlights()
        );
        result.setMaxSystemIngestionTime(acc.getMaxSystemIngestionTime());
        out.collect(result);
    }
}
