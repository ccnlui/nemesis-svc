package nemesis.svc.nanoservice;

import static nemesis.svc.nanoservice.Util.retryPublicationResult;

import java.util.concurrent.TimeUnit;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

public class BablBroadcastAgent implements Agent
{
    private final Logger LOG = LoggerFactory.getLogger(BablBroadcastAgent.class);

    private final Subscription sub;
    private final BablStreamServer bablStreamServer;
    private final FragmentHandler assembler;
    private final MutableDirectBuffer outBuf;

    private final NanoClock clock = new SystemNanoClock();
    private long nowNs = clock.nanoTime();
    private long nextReportTimeNs = nowNs;
    private long broadcastedMsg = 0;

    public BablBroadcastAgent(
        final Subscription sub,
        final BablStreamServer bablStreamServer)
    {
        this.sub = sub;
        this.bablStreamServer = bablStreamServer;
        this.assembler = new FragmentAssembler(this::onMessage);
        this.outBuf = new ExpandableDirectByteBuffer(512);
    }

    private void onMessage(DirectBuffer buffer, int offset, int length, Header header)
    {
        if (bablStreamServer.getNumClients() > 0)
        {
            long result;
            while ((result = bablStreamServer.broadcast(buffer, offset, length, header)) < 0)
            {
                if (!retryPublicationResult(result))
                    break;
            }
            broadcastedMsg += 1;
        }
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

    private boolean onScheduleReport()
    {
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
        return "bablBroadcastAgent";
    }
}
