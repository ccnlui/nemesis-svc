package nemesis.svc;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface PacketHandler
{
    void onPacket(ByteBuffer buffer);
}
