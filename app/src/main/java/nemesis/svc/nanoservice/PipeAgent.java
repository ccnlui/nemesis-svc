package nemesis.svc.nanoservice;

import static nemesis.svc.nanoservice.Util.retryPublicationResult;

import java.util.concurrent.TimeUnit;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

public class PipeAgent implements Agent
{
    private final Logger LOG = LoggerFactory.getLogger(PipeAgent.class);
    private final Subscription sub;
    private final Publication pub;
    private final FragmentHandler assembler;

    private final NanoClock clock = new SystemNanoClock();
    private long nowNs = clock.nanoTime();
    private long nextReportTimeNs = nowNs;
    private long pipedMsg = 0;

    public PipeAgent(final Subscription sub, final Publication pub)
    {
        this.sub = sub;
        this.pub = pub;
        this.assembler = new FragmentAssembler(this::onMessage);
    }

    private void onMessage(DirectBuffer buffer, int offset, int length, Header header)
    {
        if (pub.isConnected())
        {
            long result;
            while ((result = pub.offer(buffer, offset, length)) <= 0)
            {
                if (!retryPublicationResult(result))
                    break;
            }
            pipedMsg += 1;
        }
    }

    @Override
    public int doWork() throws Exception
    {
        int fragmentReceived = sub.poll(this.assembler, 10);
        if (onScheduleReport())
        {
            LOG.info("piped: {}", pipedMsg);
            pipedMsg = 0;
        }
        return fragmentReceived;
    }

    @Override
    public String roleName()
    {
        return "pipeAgent";
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
}
