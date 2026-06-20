package it.uniroma2.sae.query.q2;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Q2RankProcessor extends ProcessWindowFunction<Q2AirportResult, String, Long, TimeWindow> {
    private static final long serialVersionUID = 1L;

    @Override
    public void process(
            Long key,
            Context context,
            Iterable<Q2AirportResult> elements,
            Collector<String> out) {

        List<Q2AirportResult> list = new ArrayList<>();
        for (Q2AirportResult res : elements) {
            // Threshold constraint: only airports with at least 30 flights (completed/non-cancelled/non-diverted)
            if (res.getNumFlights() >= 30) {
                list.add(res);
            }
        }

        // Rank by number of severe delays descending
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

            // Output format: ts, rank, origin airport id, num flights, severe delays, dep delay mean, dep delay max, delayed flights
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
