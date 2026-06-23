package it.uniroma2.sae.query.rank;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import java.io.Serial;

/**
 * Handles partial emissions for Q2 global airport ranking.
 * Fired periodically by Flink's ContinuousEventTimeTrigger.
 */
public class RankAirportsGlobalWindowProcessor 
        extends ProcessWindowFunction<RankAirportsAccumulator, RankAirportsResult, Integer, GlobalWindow> {
    @Serial
    private static final long serialVersionUID = 1L;

    private static final long DATASET_START_MS = 1735689600000L; // 2025-01-01 00:00:00 UTC

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

        long currentWindowEnd = context.currentWatermark();
        if (currentWindowEnd < 0) {
            currentWindowEnd = System.currentTimeMillis();
        }

        double mean = acc.getSumDepDelay() / acc.getNumFlights();

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
