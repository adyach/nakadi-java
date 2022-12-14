package nakadi;

import java.util.Collection;

public interface PayloadSerializer {

    <T> byte[] toBytes(EventType eventType, Collection<T> payloadEntities);

    String payloadMimeType();
}
