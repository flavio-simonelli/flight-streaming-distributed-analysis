package it.uniroma2.sae.query.rank;

import it.uniroma2.sae.utils.MathUtils;
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

        out.collect(new RankAirportsResult(
                windowStartRaw, windowEndRaw, windowType,
                originAirportId,
                acc.getNumFlights(), acc.getSevereDelays(),
                mean,
                acc.getMaxDepDelay(), acc.getDelayedFlights()
        ));
    }
}
