package nakadi.avro;

import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import nakadi.BusinessEventMapped;
import nakadi.EventMetadata;
import nakadi.EventType;
import nakadi.EventTypeSchema;
import nakadi.SerializationContext;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.generated.avro.Envelope;
import org.zalando.nakadi.generated.avro.PublishingBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Objects;

public class AvroPublishingBatchSerializerTest {

    private final String schema = "{\"type\":\"record\",\"name\":\"BusinessPayload\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",\"string\"]},{\"name\":\"b\",\"type\":[\"null\",\"string\"]},{\"name\":\"id\",\"type\":[\"null\",\"string\"]}]}";

    @Test
    public void testToBytes() throws IOException {
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

        AvroPublishingBatchSerializer avroPublishingBatchSerializer = new AvroPublishingBatchSerializer();
        byte[] bytesBatch = avroPublishingBatchSerializer.toBytes(new SerializationContext() {
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

        PublishingBatch publishingBatch = PublishingBatch.fromByteBuffer(ByteBuffer.wrap(bytesBatch));
        BusinessPayload actual = new AvroMapper().reader(new AvroSchema(new Schema.Parser().parse(schema)))
                .readValue(publishingBatch.getEvents().get(0).getPayload().array(), BusinessPayload.class);

        Assert.assertEquals(bp, actual);
    }

    static class BusinessPayload {
        String id;
        String a;
        String b;

        public BusinessPayload() {
        }

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