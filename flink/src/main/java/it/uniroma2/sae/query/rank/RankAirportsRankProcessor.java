package it.uniroma2.sae.query.rank;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.io.Serial;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * KeyedProcessFunction that groups RankAirportsResult by windowEnd,
 * performs Top-10 ranking, updates the rank field, and emits JSON strings.
 */
public class RankAirportsRankProcessor extends KeyedProcessFunction<Long, RankAirportsResult, RankAirportsResult> {
    @Serial
    private static final long serialVersionUID = 1L;

    private final long rankDelayMillis;
    private transient MapState<Integer, RankAirportsResult> windowState;

    public RankAirportsRankProcessor(Duration rankDelay) {
        this.rankDelayMillis = rankDelay.toMillis();
    }

    @Override
    public void open(OpenContext context) {
        windowState = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "rank-window-state",
                Integer.class,
                RankAirportsResult.class
        ));
    }

    @Override
    public void processElement(RankAirportsResult value, Context ctx, Collector<RankAirportsResult> out) throws Exception {
        windowState.put(value.getOriginAirportId(), value);
        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + rankDelayMillis);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RankAirportsResult> out) throws Exception {
        PriorityQueue<RankAirportsResult> pq = new PriorityQueue<>(11,
                (a, b) -> Integer.compare(a.getSevereDelays(), b.getSevereDelays())
        );

        for (RankAirportsResult res : windowState.values()) {
            if (res.getNumFlights() >= 30) {
                pq.add(res);
                if (pq.size() > 10) {
                    pq.poll();
                }
            }
        }

        windowState.clear();

        // Retrieve and sort the top 10 descending
        List<RankAirportsResult> top10 = new ArrayList<>(pq);
        top10.sort((a, b) -> Integer.compare(b.getSevereDelays(), a.getSevereDelays()));

        int rank = 1;
        for (RankAirportsResult res : top10) {
            res.setRank(rank++);
            out.collect(res);
        }
    }
}
