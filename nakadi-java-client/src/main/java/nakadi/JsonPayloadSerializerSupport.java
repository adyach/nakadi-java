package nakadi;

public class JsonPayloadSerializerSupport implements PayloadSerializerSupport {

    private JsonSupport jsonSupport;

    public JsonPayloadSerializerSupport(JsonSupport jsonSupport) {
        this.jsonSupport = jsonSupport;
    }

    @Override
    public PayloadSerializer newInstance() {
        return new JsonPayloadSerializer(jsonSupport);
    }
}
