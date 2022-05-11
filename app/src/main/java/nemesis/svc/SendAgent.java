package nemesis.svc;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Publication;
import nemesis.svc.message.Message;

public class SendAgent implements Agent
{
    private final Logger LOG = LoggerFactory.getLogger(SendAgent.class);
    private final Publication pub;
    private final Message msg;
    private final UnsafeBuffer unsafeBuffer;

    private final NanoClock clock = new SystemNanoClock();
    private final long intervalNs;
    private final long startTimeNs = clock.nanoTime();
    private long nowNs = startTimeNs;
    private long nextSendTimeNs = startTimeNs;
    private long nextReportTimeNs = startTimeNs;
    private long sentMsg = 0;

    public SendAgent(final Publication pub, final Message msg, final long intervalNs)
    {
        this.pub = pub;
        this.msg = msg;
        this.intervalNs = intervalNs;
        this.unsafeBuffer = new UnsafeBuffer(msg.byteBuffer());
    }

    @Override
    public int doWork() throws Exception
    {
        if (pub.isConnected() && onScheduleSend())
        {
            long epochNs = nowNano();
            msg.setTimestamp(epochNs - 1_000_000L);
            msg.setReceivedAt(epochNs);

            long pos = pub.offer(unsafeBuffer);
            if (pos < 0)
            {
                LOG.error("failed to send message: {}", Publication.errorString(pos));
            }
            sentMsg += 1;
        }
        if (onScheduleReport())
        {
            LOG.info("sent: {}", sentMsg);
            sentMsg = 0;
        }
        return 0;
    }

    @Override
    public String roleName()
    {
        if (msg.type() == Message.QUOTE)
            return "quoteSender";
        else
            return "tradeSender";
    }

    private boolean onScheduleSend()
    {
        nowNs = clock.nanoTime();
        if (nextSendTimeNs <= nowNs)
        {
            nextSendTimeNs += intervalNs;
            return true;
        }
        return false;
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

    private long nowNano()
    {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }
}
