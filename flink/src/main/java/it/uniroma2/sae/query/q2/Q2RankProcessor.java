package it.uniroma2.sae.query.q2;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serial;
import java.util.ArrayList;
import java.util.List;

/**
 * Evaluates incoming airport summaries matching identical window scopes.
 * Implements an event-time evaluation timer to trigger ordered top-10 selections.
 */
public class Q2RankProcessor extends KeyedProcessFunction<Long, Q2AirportResult, String> {

    @Serial
    private static final long serialVersionUID = 1L;

    private transient ListState<Q2AirportResult> windowState;

    @Override
    public void open(OpenContext context) {
        windowState = getRuntimeContext().getListState(new ListStateDescriptor<>("rank-window-state", Q2AirportResult.class));
    }

    @Override
    public void processElement(Q2AirportResult value, Context ctx, Collector<String> out) throws Exception {
        windowState.add(value);
        // Register a timer at the windowEnd timestamp to perform ranking once all elements arrive
        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<Q2AirportResult> list = new ArrayList<>();
        for (Q2AirportResult res : windowState.get()) {
            if (res.getNumFlights() >= 30) {
                list.add(res);
            }
        }

        // Clear state once fetched to maintain strict memory balance
        windowState.clear();

        list.sort((a, b) -> Integer.compare(b.getSevereDelays(), a.getSevereDelays()));

        int rank = 1;
        for (int i = 0; i < Math.min(10, list.size()); i++) {
            Q2AirportResult res = list.get(i);

            StringBuilder sb = new StringBuilder();
            sb.append("[");
            List<Q2DelayedFlight> delayed = res.getDelayedFlights();
            for (int j = 0; j < delayed.size(); j++) {
                if (j > 0) {
                    sb.append(", ");
                }
                sb.append(delayed.get(j).toString());
            }
            sb.append("]");

            String csv = String.format("%d, %d, %d, %d, %d, %.2f, %.2f, %s",
                    res.getWindowStart(),
                    rank++,
                    res.getOriginAirportId(),
                    res.getNumFlights(),
                    res.getSevereDelays(),
                    res.getDepDelayMean(),
                    res.getDepDelayMax(),
                    sb.toString()
            );
            out.collect(csv);
        }
    }
}