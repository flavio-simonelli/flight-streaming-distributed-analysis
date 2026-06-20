package it.uniroma2.sae;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.FlightRecord;
import it.uniroma2.sae.sink.SinkBuilder;
import it.uniroma2.sae.source.SourceBuilder;
import it.uniroma2.sae.source.deserializer.FlightRecordDeserializationSchema;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Flink Test Job logic. Runs in BATCH mode using ApplicationConfig.
 */
public class FlightDelayJob implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(FlightDelayJob.class);

    private final ApplicationConfig config;

    public FlightDelayJob(ApplicationConfig config) {
        this.config = config;
    }

    public void defineJob(StreamExecutionEnvironment env) {
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        KafkaSource<FlightRecord> source = new SourceBuilder<FlightRecord>(
                config.getKafka(),
                new FlightRecordDeserializationSchema()
        )
        .setBounded(OffsetsInitializer.latest())
        .build();

        KafkaSink<String> sink = new SinkBuilder(config.getKafka()).build();

        LOG.info("Defining BATCH job: Source={} -> Sink={}", config.getKafka().getInputTopic(), config.getKafka().getSinkTopic());

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .filter(record -> record != null)
                .map(new FlightToTupleMapper())
                .returns(TypeInformation.of(new TypeHint<Tuple2<Double, Long>>(){}))
                .keyBy(t -> "global")
                .reduce(new SumReducer())
                .map(new ResultMapper())
                .sinkTo(sink);
    }

    public static class FlightToTupleMapper implements MapFunction<FlightRecord, Tuple2<Double, Long>> {
        @Override
        public Tuple2<Double, Long> map(FlightRecord r) {
            return new Tuple2<>(r.getDepDelay(), 1L);
        }
    }

    public static class SumReducer implements ReduceFunction<Tuple2<Double, Long>> {
        @Override
        public Tuple2<Double, Long> reduce(Tuple2<Double, Long> t1, Tuple2<Double, Long> t2) {
            return new Tuple2<>(t1.f0 + t2.f0, t1.f1 + t2.f1);
        }
    }

    public static class ResultMapper implements MapFunction<Tuple2<Double, Long>, String> {
        @Override
        public String map(Tuple2<Double, Long> result) {
            double avg = result.f0 / result.f1;
            return String.valueOf(avg);
        }
    }
}
