package nemesis.svc.nanoservice;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface PacketHandler
{
    void onPacket(ByteBuffer buffer);
}
