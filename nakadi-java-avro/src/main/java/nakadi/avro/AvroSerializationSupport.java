package nakadi.avro;

import nakadi.EventType;
import nakadi.EventTypeSchema;
import nakadi.NakadiClient;
import nakadi.SerializationContext;
import nakadi.SerializationSupport;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AvroSerializationSupport implements SerializationSupport {

    private final AvroPayloadSerializer payloadSerializer;
    private final Map<String, SerializationContext> contextCache;

    public AvroSerializationSupport(AvroPayloadSerializer payloadSerializer) {
        this.payloadSerializer = payloadSerializer;
        this.contextCache = new ConcurrentHashMap<>();
    }

    public static SerializationSupport newInstance() {
        return new AvroSerializationSupport(new AvroPayloadSerializer());
    }

    @Override
    public <T> byte[] serializePayload(NakadiClient client, String eventTypeName, Collection<T> events) {
        SerializationContext context = contextCache.computeIfAbsent(eventTypeName,
                (et) -> new AvroSerializationContext(client.resources()
                        .eventTypes().findByName(eventTypeName)));
        return payloadSerializer.toBytes(context, events);
    }

    @Override
    public String contentType() {
        return "application/avro-binary";
    }

    private class AvroSerializationContext implements SerializationContext {

        private final EventType eventType;

        private AvroSerializationContext(EventType eventType) {
            if (eventType.schema().type() != EventTypeSchema.Type.avro_schema) {
                throw new InvalidSchemaException(String.format("No Avro schema found for event type `%s`. " +
                        "Update the event type to have latest schema as Avro schema ", eventType.name()));
            }

            this.eventType = eventType;
        }

        @Override
        public String name() {
            return eventType.name();
        }

        @Override
        public String schema() {
            return eventType.schema().schema();
        }

        @Override
        public String version() {
            return eventType.schema().version();
        }

    }
}
