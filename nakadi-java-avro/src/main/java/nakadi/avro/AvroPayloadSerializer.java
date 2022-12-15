package nakadi.avro;

import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import nakadi.BusinessEventMapped;
import nakadi.DataChangeEvent;
import nakadi.Event;
import nakadi.EventMetadata;
import nakadi.EventType;
import nakadi.EventTypeSchema;
import nakadi.PayloadSerializer;
import org.apache.avro.Schema;
import org.zalando.nakadi.generated.avro.Envelope;
import org.zalando.nakadi.generated.avro.Metadata;
import org.zalando.nakadi.generated.avro.PublishingBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The serializer uses jackson extension to serialize business pojos to avro events.
 */
public class AvroPayloadSerializer implements PayloadSerializer {

    private AvroMapper avroMapper;

    public AvroPayloadSerializer() {
        this.avroMapper = new AvroMapper();
    }

    @Override
    public <T> byte[] toBytes(EventType eventType, Collection<T> events) {
        final EventTypeSchema etSchema = eventType.schema();
        if (etSchema.type() != EventTypeSchema.Type.avro_schema) {
            throw new InvalidSchemaException(String.format("No Avro schema found for event type `%s`. " +
                    "Update the event type to have latest schema as Avro schema ", eventType.name()));
        }

        try {
            final List<Envelope> envelops = events.stream()
                    .map(event -> toEnvelope(eventType, event))
                    .collect(Collectors.toList());
            return PublishingBatch.newBuilder().setEvents(envelops)
                    .build().toByteBuffer().array();
        } catch (IOException io) {
            throw new RuntimeException();
        }
    }

    private <T> Envelope toEnvelope(EventType eventType, T event) {
        try {
            final EventMetadata metadata;
            final Object data;
            if (event instanceof BusinessEventMapped) {
                metadata = ((BusinessEventMapped) event).metadata();
                data = ((BusinessEventMapped) event).data();
            } else if (event instanceof DataChangeEvent) {
                metadata = ((DataChangeEvent) event).metadata();
                data = ((DataChangeEvent) event).data();
            } else {
                throw new InvalidEventTypeException("Unexpected event category `" +
                        event.getClass() + "` provided during avro serialization");
            }

            final byte[] eventBytes = avroMapper.writer(
                    new AvroSchema(new Schema.Parser().parse(eventType.schema().schema())))
                    .writeValueAsBytes(data);

            return Envelope.newBuilder()
                    .setMetadata(Metadata.newBuilder()
                            .setEventType(metadata.eventType())
                            .setVersion(eventType.schema().version())
                            .setOccurredAt(metadata.occurredAt().toInstant())
                            .setEid(metadata.eid())
                            .setPartition(metadata.partition())
                            .setPartitionCompactionKey(metadata.partitionCompactionKey())
                            .build())
                    .setPayload(ByteBuffer.wrap(eventBytes))
                    .build();
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
    }

    @Override
    public String payloadMimeType() {
        return "application/avro-binary";
    }
}
