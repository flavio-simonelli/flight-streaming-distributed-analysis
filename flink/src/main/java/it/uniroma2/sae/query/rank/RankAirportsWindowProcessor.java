package it.uniroma2.sae.query.rank;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.io.Serial;

/**
 * Window processor for RankAirportsQuery.
 * Emits RankAirportsResult with specific window type label.
 */
public class RankAirportsWindowProcessor 
        extends ProcessWindowFunction<RankAirportsAccumulator, RankAirportsResult, Integer, TimeWindow> {
    @Serial
    private static final long serialVersionUID = 1L;

    private final String windowType;

    public RankAirportsWindowProcessor(String windowType) {
        this.windowType = windowType;
    }

    @Override
    public void process(
            Integer key,
            Context context,
            Iterable<RankAirportsAccumulator> elements,
            Collector<RankAirportsResult> out) {

        RankAirportsAccumulator acc = elements.iterator().next();
        long start = context.window().getStart();
        long end = context.window().getEnd();

        double mean = acc.getNumFlights() > 0 ? acc.getSumDepDelay() / acc.getNumFlights() : 0.0;

        out.collect(new RankAirportsResult(
                start,
                end,
                windowType,
                key,
                acc.getNumFlights(),
                acc.getSevereDelays(),
                mean,
                acc.getMaxDepDelay(),
                acc.getDelayedFlights()
        ));
    }
}
