package it.uniroma2.sae.query.rank;

import it.uniroma2.sae.utils.MathUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import java.io.Serial;
import java.time.Duration;

/**
 * Handles partial emissions for Q2 global airport ranking.
 * Fired periodically by Flink's ContinuousEventTimeTrigger.
 */
public class RankAirportsGlobalWindowProcessor 
        extends ProcessWindowFunction<RankAirportsAccumulator, RankAirportsResult, Integer, GlobalWindow> {
    @Serial
    private static final long serialVersionUID = 1L;

    private static final long DATASET_START_MS = 1735689600000L; // 2025-01-01 00:00:00 UTC
    private final Duration triggerInterval;

    public RankAirportsGlobalWindowProcessor(Duration triggerInterval) {
        this.triggerInterval = triggerInterval;
    }

    @Override
    public void process(
            Integer key,
            Context context,
            Iterable<RankAirportsAccumulator> elements,
            Collector<RankAirportsResult> out) {

        RankAirportsAccumulator acc = elements.iterator().next();

        // Vincolo dei 30 voli minimi richiesto dalla traccia
        if (acc.getNumFlights() < 30) {
            return;
        }

        long watermark = context.currentWatermark();
        if (watermark < 0) {
            watermark = System.currentTimeMillis();
        }
        // Round down to the nearest trigger interval boundary to align parallel subtasks
        long intervalMs = triggerInterval.toMillis();
        long currentWindowEnd = (watermark / intervalMs) * intervalMs;

        double mean = MathUtils.safeDivideRounded(acc.getSumDepDelay(), acc.getNumFlights());

        out.collect(new RankAirportsResult(
                DATASET_START_MS, // window_start dataset start
                currentWindowEnd, // window_end avanza di ora in ora
                "global",
                key,
                acc.getNumFlights(),
                acc.getSevereDelays(),
                mean,
                acc.getMaxDepDelay(),
                acc.getDelayedFlights()
        ));
    }
}
