package nakadi;

import java.util.Collection;

public class JsonSerializationSupport implements SerializationSupport {

    private final JsonPayloadSerializer payloadSerializer;

    public JsonSerializationSupport(JsonPayloadSerializer payloadSerializer) {
        this.payloadSerializer = payloadSerializer;
    }

    public static SerializationSupport newInstance(JsonSupport jsonSupport) {
        return new JsonSerializationSupport(new JsonPayloadSerializer(jsonSupport));
    }

    @Override
    public <T> byte[] serializePayload(NakadiClient client, String eventTypeName, Collection<T> events) {
        SerializationContext context = new SerializationContext() {
            @Override
            public String name() {
                return eventTypeName;
            }

            @Override
            public String schema() {
                return null;
            }

            @Override
            public String version() {
                return null;
            }
        };

        return payloadSerializer.toBytes(context, events);
    }

    @Override
    public String contentType() {
        return ResourceSupport.APPLICATION_JSON_CHARSET_UTF_8;
    }
}
