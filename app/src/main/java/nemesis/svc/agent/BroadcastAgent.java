package nemesis.svc.agent;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import nemesis.svc.nanoservice.StreamServer;

public class BroadcastAgent implements Agent
{
    private final Logger LOG = LoggerFactory.getLogger(BroadcastAgent.class);
    private final Subscription sub;
    private final StreamServer streamServer;
    private final FragmentHandler assembler;

    private final ByteBuffer outBuf;

    private final NanoClock clock = new SystemNanoClock();
    private long nowNs = clock.nanoTime();
    private long nextReportTimeNs = nowNs;
    private long broadcastedMsg = 0;

    public BroadcastAgent(
        final Subscription sub,
        final StreamServer streamServer)
    {
        this.sub = sub;
        this.streamServer = streamServer;
        this.assembler = new FragmentAssembler(this::onMessage);
        this.outBuf = ByteBuffer.allocateDirect(512);
    }

    private void onMessage(DirectBuffer buffer, int offset, int length, Header header)
    {
        this.outBuf.clear();
        buffer.getBytes(offset, outBuf, 0, length);
        this.outBuf.limit(length);
        streamServer.broadcast(outBuf);
        broadcastedMsg += 1;
    }

    @Override
    public int doWork() throws Exception
    {
        int fragmentReceived = sub.poll(this.assembler, 10);
        if (onScheduleReport())
        {
            LOG.info("broadcasted: {}", broadcastedMsg);
            broadcastedMsg = 0;
        }
        return fragmentReceived;
    }

    private boolean onScheduleReport() {
        nowNs = clock.nanoTime();
        if (nextReportTimeNs <= nowNs)
        {
            nextReportTimeNs += TimeUnit.SECONDS.toNanos(1);
            return true;
        }
        return false;
    }

    @Override
    public String roleName()
    {
        return "broadcastAgent";
    }
}
