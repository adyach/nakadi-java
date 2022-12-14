package nakadi.avro;

import junit.framework.TestCase;
import nakadi.BusinessEventMapped;
import nakadi.EventMetadata;
import nakadi.EventResource;
import nakadi.NakadiClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

public class EventResourceRealTest {

    public static final int MOCK_SERVER_PORT = 8317;

    static class Happened {
        String id;

        public Happened(String id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Happened happened = (Happened) o;
            return Objects.equals(id, happened.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    MockWebServer server = new MockWebServer();

    // these two are not annotated as we don't want to open a server for every test

    public void before() {
        try {
            server.start(InetAddress.getByName("localhost"), MOCK_SERVER_PORT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void after() {
        try {
            server.shutdown();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void businessIsSentToServerMapped() throws Exception {

        NakadiClient client = spy(NakadiClient.newBuilder()
                .baseURI("http://localhost:" + MOCK_SERVER_PORT)
                .payloadSerializer(new AvroPayloadSerializer())
                .build());

        EventResource resource = client.resources().events();

        BusinessPayload bp = new BusinessPayload("22", "A", "B");

        BusinessEventMapped<BusinessPayload> event =
                new BusinessEventMapped<BusinessPayload>()
                        .metadata(EventMetadata.newPreparedEventMetadata())
                        .data(bp);

        try {
            before();

            server.enqueue(new MockResponse().setResponseCode(200));
            resource.send("ad-2022-12-13", event);
            RecordedRequest request = server.takeRequest();

            assertEquals("POST /event-types/ad-2022-12-13/events HTTP/1.1", request.getRequestLine());
            assertEquals("application/avro-binary", request.getHeaders().get("Content-Type"));
            TestCase.assertTrue(request.getHeaders().get("X-Flow-Id") != null);

            String body = request.getBody().readUtf8();


            assertEquals(1, body.getBytes(StandardCharsets.UTF_8).length);

      /*
       we're expecting a business event type's "data" fields to get lifted out to the
       top level of the json doc, so it should match the raw map versions of the enclosed
       BusinessPayload plus a metadata field
        */
//            Map<String, Object> businessAsMap = sent.get(0);
//            assertEquals(4, businessAsMap.size());
//            assertEquals("22", businessAsMap.get("id"));
//            assertEquals("A", businessAsMap.get("a"));
//            assertEquals("B", businessAsMap.get("b"));
//            assertEquals(3, ((Map) businessAsMap.get("metadata")).size());
//            assertTrue(((Map) businessAsMap.get("metadata")).containsKey("eid"));
//            assertTrue(((Map) businessAsMap.get("metadata")).containsKey("occurred_at"));
//            assertTrue(((Map) businessAsMap.get("metadata")).containsKey("flow_id"));
//            assertTrue(((Map) businessAsMap.get("metadata")).get("flow_id").toString().startsWith("njc"));

        } finally {
            after();
        }
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
