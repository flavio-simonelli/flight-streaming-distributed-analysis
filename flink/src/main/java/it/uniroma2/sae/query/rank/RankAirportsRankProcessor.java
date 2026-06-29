package it.uniroma2.sae.query.rank;

import it.uniroma2.sae.metrics.ProcessingLatencyTracker;
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

    private transient ProcessingLatencyTracker latencyTracker;

    @Override
    public void open(OpenContext context) {
        windowState = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "rank-window-state",
                Integer.class,
                RankAirportsResult.class
        ));
        this.latencyTracker = new ProcessingLatencyTracker("q2_rank");
        this.latencyTracker.register(getRuntimeContext().getMetricGroup());
    }

    @Override
    public void processElement(RankAirportsResult value, Context ctx, Collector<RankAirportsResult> out) throws Exception {
        windowState.put(value.getOriginAirportId(), value);
        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + rankDelayMillis);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RankAirportsResult> out) throws Exception {
        long start = System.currentTimeMillis();
        long maxIngestion = 0L;
        long minIngestion = Long.MAX_VALUE;
        long sumIngestion = 0L;
        long countIngestion = 0L;
        for (RankAirportsResult res : windowState.values()) {
            maxIngestion = Math.max(maxIngestion, res.getMaxSystemIngestionTime());
            if (res.getMinSystemIngestionTime() != Long.MAX_VALUE) {
                minIngestion = Math.min(minIngestion, res.getMinSystemIngestionTime());
            }
            sumIngestion += res.getSumSystemIngestionTime();
            countIngestion += res.getSystemIngestionTimeCount();
        }
        long avgIngest = countIngestion > 0 ? (sumIngestion / countIngestion) : 0L;

        try {
            onTimerInternal(timestamp, ctx, out);
        } finally {
            long duration = System.currentTimeMillis() - start;
            if (latencyTracker != null) {
                latencyTracker.updateOperator(duration);
                latencyTracker.updateE2E(maxIngestion, minIngestion, avgIngest);
            }
        }
    }

    private void onTimerInternal(long timestamp, OnTimerContext ctx, Collector<RankAirportsResult> out) throws Exception {
        // Use a min-heap with deterministic tie-breakers (1st: severeDelays, 2nd: depDelayMean, 3rd: originAirportId)
        PriorityQueue<RankAirportsResult> pq = new PriorityQueue<>(11, (a, b) -> {
            int cmp = Integer.compare(a.getSevereDelays(), b.getSevereDelays());
            if (cmp != 0) return cmp;
            int cmpMean = Double.compare(a.getDepDelayMean(), b.getDepDelayMean());
            if (cmpMean != 0) return cmpMean;
            return Integer.compare(b.getOriginAirportId(), a.getOriginAirportId()); // Ascending order in final rank, so descending in min-heap
        });

        for (RankAirportsResult res : windowState.values()) {
            if (res.getNumFlights() >= 30) {
                pq.add(res);
                if (pq.size() > 10) {
                    pq.poll();
                }
            }
        }

        windowState.clear();

        // Retrieve and sort the top 10 descending (with identical tie-breakers)
        List<RankAirportsResult> top10 = new ArrayList<>(pq);
        top10.sort((a, b) -> {
            int cmp = Integer.compare(b.getSevereDelays(), a.getSevereDelays());
            if (cmp != 0) return cmp;
            int cmpMean = Double.compare(b.getDepDelayMean(), a.getDepDelayMean());
            if (cmpMean != 0) return cmpMean;
            return Integer.compare(a.getOriginAirportId(), b.getOriginAirportId());
        });

        int rank = 1;
        for (RankAirportsResult res : top10) {
            res.setRank(rank++);
            out.collect(res);
        }
    }
}
