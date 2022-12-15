package nakadi.avro;

import nakadi.BusinessEventMapped;
import nakadi.EventMetadata;
import nakadi.EventType;
import nakadi.EventTypeSchema;
import nakadi.SerializationContext;
import org.junit.Test;

import java.util.Collections;
import java.util.Objects;

public class AvroPayloadSerializerTest {

    private final String schema = "{\"type\":\"record\",\"name\":\"BusinessPayload\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",\"string\"]},{\"name\":\"b\",\"type\":[\"null\",\"string\"]},{\"name\":\"id\",\"type\":[\"null\",\"string\"]}]}";

    @Test
    public void testToBytes() {
        final String version = "1.0.0";
        EventTypeSchema eventTypeSchema = new EventTypeSchema()
                .type(EventTypeSchema.Type.avro_schema)
                .schema(schema)
                .version(version);
        String name = "ad-2022-12-13";
        EventType eventType = new EventType().name(name);
        eventType.schema(eventTypeSchema);

        BusinessPayload bp = new BusinessPayload("22", "A", "B");
        BusinessEventMapped<BusinessPayload> event =
                new BusinessEventMapped<BusinessPayload>()
                        .metadata(EventMetadata.newPreparedEventMetadata().eventType(name))
                        .data(bp);

        AvroPayloadSerializer avroPayloadSerializer = new AvroPayloadSerializer();
        byte[] bytes = avroPayloadSerializer.toBytes(new SerializationContext() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public String schema() {
                return schema;
            }

            @Override
            public String version() {
                return version;
            }
        }, Collections.singletonList(event));

        System.out.println(new String(bytes));
    }

    static class BusinessPayload {
        String id;
        String a;
        String b;

        public BusinessPayload(String id, String a, String b) {
            this.id = id;
            this.a = a;
            this.b = b;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getA() {
            return a;
        }

        public void setA(String a) {
            this.a = a;
        }

        public String getB() {
            return b;
        }

        public void setB(String b) {
            this.b = b;
        }

        @Override
        public String toString() {
            return "BusinessPayload{" + "id='" + id + '\'' +
                    ", a='" + a + '\'' +
                    ", b='" + b + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BusinessPayload that = (BusinessPayload) o;
            return Objects.equals(id, that.id) &&
                    Objects.equals(a, that.a) &&
                    Objects.equals(b, that.b);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, a, b);
        }
    }
}