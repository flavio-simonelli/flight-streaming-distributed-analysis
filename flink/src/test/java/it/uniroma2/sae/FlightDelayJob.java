package it.uniroma2.sae;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.FlightRecord;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

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
        String kafkaBootstrap = config.getKafka().getHost() + ":" + config.getKafka().getInternalPort();
        String inputTopic = config.getKafka().getInputTopic();
        String sinkTopic = config.getKafka().getSinkTopic();
        String groupId = config.getKafka().getGroupId();

        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setBounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(new RecordSerializer(sinkTopic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        LOG.info("Defining BATCH job: Source={} -> Sink={}", inputTopic, sinkTopic);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(new JsonMapper())
                .filter(record -> record != null && record.getDepDelay() != null)
                .map(new FlightToTupleMapper())
                .returns(TypeInformation.of(new TypeHint<Tuple2<Double, Long>>(){}))
                .keyBy(t -> "global")
                .reduce(new SumReducer())
                .map(new ResultMapper())
                .sinkTo(sink);
    }

    public static class RecordSerializer implements KafkaRecordSerializationSchema<String>, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private final String topic;

        public RecordSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
            return new ProducerRecord<>(
                    this.topic,
                    null,
                    null,
                    null,
                    element.getBytes(StandardCharsets.UTF_8)
            );
        }
    }

    public static class JsonMapper implements MapFunction<String, FlightRecord> {
        private static final ObjectMapper MAPPER = new ObjectMapper();
        @Override
        public FlightRecord map(String json) {
            try {
                return MAPPER.readValue(json, FlightRecord.class);
            } catch (Exception e) {
                return null;
            }
        }
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
