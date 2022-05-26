package nemesis.svc.nanoservice;

import static java.lang.Math.max;

import java.nio.ByteBuffer;

import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import nemesis.svc.message.Message;
import nemesis.svc.message.Quote;
import nemesis.svc.message.Trade;

public class ReceiveAgent implements Agent
{
    private final Logger LOG = LoggerFactory.getLogger(ReceiveAgent.class);
    private final Subscription sub;
    private final UnsafeBuffer unsafeBuffer;
    private final Quote quote;
    private final Trade trade;
    private final Histogram histogram;
    private final FragmentHandler assembler;
    private final ShutdownSignalBarrier barrier;

    private final NanoClock clock = new SystemNanoClock();
    private final EpochNanoClock epochClock = new OffsetEpochNanoClock();
    private long nowNs = clock.nanoTime();
    private final long startTimeNs = nowNs + 10_000_000_000L;
    private final long endTimeNs = nowNs + 30_000_000_000L;
    

    public ReceiveAgent(
        final Subscription sub,
        final Histogram histogram,
        final ShutdownSignalBarrier barrier)
    {
        this.sub = sub;
        this.unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Message.MAX_SIZE));
        this.quote = new Quote();
        this.trade = new Trade();
        this.histogram = histogram;
        this.assembler = new FragmentAssembler(this::onMessage);
        this.barrier = barrier;
    }

    private void onMessage(DirectBuffer buffer, int offset, int length, Header header)
    {
        buffer.getBytes(offset, unsafeBuffer, 0, length);

        int msgType = unsafeBuffer.getByte(0);
        switch (msgType)
        {
        case Message.QUOTE:
            this.quote.fromByteBuffer(unsafeBuffer.byteBuffer());
            if (this.onScheduleMeasure())
            {
                histogram.recordValue(max(1, epochClock.nanoTime() - quote.receivedAt()));
            }
            // LOG.info("quote: {}", quote.receivedAt());
            break;

        case Message.TRADE:
            this.trade.fromByteBuffer(unsafeBuffer.byteBuffer());
            if (this.onScheduleMeasure())
            {
                histogram.recordValue(max(1, epochClock.nanoTime() - trade.receivedAt()));
            }
            // LOG.info("trade: {}", trade.receivedAt());
            break;

        default:
            LOG.error("unexpected message type: {}", msgType);
        }
    }

    @Override
    public int doWork() throws Exception
    {
        int fragmentReceived = this.sub.poll(this.assembler, 10);
        if (endTimeNs <= nowNs)
        {
            barrier.signal();
        }
        return fragmentReceived;
    }

    @Override
    public String roleName()
    {
        return "receiver";
    }

    private boolean onScheduleMeasure()
    {
        nowNs = clock.nanoTime();
        if (startTimeNs <= nowNs)
        {
            return true;
        }
        return false;
    }
}
