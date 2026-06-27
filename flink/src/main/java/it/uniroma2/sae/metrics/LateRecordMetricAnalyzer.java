package it.uniroma2.sae.metrics;

import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;

/**
 * Custom ProcessFunction to measure, log, and report the event-time lateness distribution
 * of dropped/late FlightRecord events using Flink's Histogram metrics.
 */
public class LateRecordMetricAnalyzer extends ProcessFunction<FlightRecord, FlightRecord> implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final long windowSizeMs;
    private final long allowedLatenessMs;

    private transient Histogram dropLatenessHistogram;

    public LateRecordMetricAnalyzer(Duration windowSize, Duration allowedLateness) {
        this.windowSizeMs = windowSize.toMillis();
        this.allowedLatenessMs = allowedLateness.toMillis();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Register a Flink Histogram metric to track lateness distribution in minutes
        this.dropLatenessHistogram = getRuntimeContext()
                .getMetricGroup()
                .histogram("dropped_lateness_minutes", new DescriptiveStatisticsHistogram(1024));
    }

    @Override
    public void processElement(FlightRecord record, Context ctx, Collector<FlightRecord> out) throws Exception {
        Long eventTime = ctx.timestamp();
        if (eventTime == null) {
            out.collect(record);
            return;
        }

        long watermark = ctx.timerService().currentWatermark();

        long windowStart = eventTime - (eventTime % windowSizeMs);
        long windowEnd = windowStart + windowSizeMs;
        long windowDeadline = windowEnd + allowedLatenessMs;

        // Calculate by how many minutes the record missed its deadline
        double latenessMinutes = (watermark - windowDeadline) / 60000.0;

        if (latenessMinutes > 0) {
            // Update the Flink Histogram metric
            dropLatenessHistogram.update((long) latenessMinutes);
        }

        // Output the record (e.g. for forwarding to a dead-letter queue or logging)
        out.collect(record);
    }
}
