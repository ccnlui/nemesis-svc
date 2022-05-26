package nemesis.svc.nanoservice;

import static nemesis.svc.nanoservice.Util.retryPublicationResult;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import nemesis.svc.message.Message;
import nemesis.svc.message.Quote;
import nemesis.svc.message.Trade;

public class MarshalAgent implements Agent
{
    private final Logger LOG = LoggerFactory.getLogger(MarshalAgent.class);
    private final Subscription sub;
    private final Publication pub;
    private final FragmentHandler assembler;

    private final UnsafeBuffer inBuf;
    private final UnsafeBuffer outBuf;
    private final Quote quote;
    private final Trade trade;
    private final Message.Format format;

    private final NanoClock clock = new SystemNanoClock();
    private long nowNs = clock.nanoTime();
    private long nextReportTimeNs = nowNs;
    private long marshalledMsg = 0;

    public MarshalAgent(
        final Subscription sub,
        final Publication pub,
        final Message.Format format)
    {
        this.sub = sub;
        this.pub = pub;
        this.assembler = new FragmentAssembler(this::onMessage);
        this.inBuf = new UnsafeBuffer(ByteBuffer.allocateDirect(Message.MAX_SIZE));
        this.outBuf = new UnsafeBuffer(ByteBuffer.allocateDirect(Message.MAX_SIZE));
        this.quote = new Quote();
        this.trade = new Trade();
        this.format = format;
    }

    private void onMessage(DirectBuffer buffer, int offset, int length, Header header)
    {
        int outBytes;

        buffer.getBytes(offset, inBuf, 0, length);
        int msgType = inBuf.getByte(0);
        switch (msgType)
        {
        case Message.QUOTE:
            this.quote.fromByteBuffer(inBuf.byteBuffer());
            outBytes = this.quote.toMessageData(format, outBuf);
            break;

        case Message.TRADE:
            this.trade.fromByteBuffer(inBuf.byteBuffer());
            outBytes = this.trade.toMessageData(format, outBuf);
            break;

        default:
            LOG.error("unexpected message type: {}", msgType);
            return;
        }

        if (pub.isConnected())
        {
            long result;
            while ((result = pub.offer(outBuf, 0, outBytes)) <= 0)
            {
                if (!retryPublicationResult(result))
                    break;
            }
            marshalledMsg += 1;
        }
    }

    @Override
    public int doWork() throws Exception
    {
        int fragmentReceived = sub.poll(this.assembler, 10);
        if (onScheduleReport())
        {
            LOG.info("marshalled: {}", marshalledMsg);
            marshalledMsg = 0;
        }
        return fragmentReceived;
    }

    @Override
    public String roleName()
    {
        return "marshalAgent";
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
