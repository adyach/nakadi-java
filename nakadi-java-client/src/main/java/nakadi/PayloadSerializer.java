package nakadi;

import java.util.Collection;

public interface PayloadSerializer {

    <T> byte[] toBytes(SerializationContext context, Collection<T> payloadEntities);

}
