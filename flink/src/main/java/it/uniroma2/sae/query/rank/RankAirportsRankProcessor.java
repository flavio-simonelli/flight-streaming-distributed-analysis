package it.uniroma2.sae.query.rank;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.io.Serial;
import java.util.ArrayList;
import java.util.List;

/**
 * KeyedProcessFunction that groups RankAirportsResult by windowEnd,
 * performs Top-10 ranking, updates the rank field, and emits JSON strings.
 */
public class RankAirportsRankProcessor extends KeyedProcessFunction<Long, RankAirportsResult, String> {
    @Serial
    private static final long serialVersionUID = 1L;

    private transient ListState<RankAirportsResult> windowState;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void open(OpenContext context) {
        windowState = getRuntimeContext().getListState(new ListStateDescriptor<>("rank-window-state", RankAirportsResult.class));
    }

    @Override
    public void processElement(RankAirportsResult value, Context ctx, Collector<String> out) throws Exception {
        windowState.add(value);
        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<RankAirportsResult> list = new ArrayList<>();
        for (RankAirportsResult res : windowState.get()) {
            if (res.getNumFlights() >= 30) {
                list.add(res);
            }
        }

        windowState.clear();

        // Sort descending by severe delays count
        list.sort((a, b) -> Integer.compare(b.getSevereDelays(), a.getSevereDelays()));

        int rank = 1;
        for (int i = 0; i < Math.min(10, list.size()); i++) {
            RankAirportsResult res = list.get(i);
            res.setRank(rank++);
            out.collect(MAPPER.writeValueAsString(res));
        }
    }
}
