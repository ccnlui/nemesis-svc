package nemesis.svc.nanoservice;

import static java.lang.Math.max;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

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
    private final io.prometheus.client.Histogram msgDelay;

    private final NanoClock clock;
    private final EpochNanoClock epochClock;
    private long nowNs;
    private final long startTimeNs;
    private final long endTimeNs;
    private final long resetIntervalNs;
    private final long reportIntervalNs;
    private long nextResetTimeNs;
    private long nextReportTimeNs;
    

    public ReceiveAgent(
        final Subscription sub,
        final Histogram histogram,
        final ShutdownSignalBarrier barrier,
        final long testDurationNs,
        final io.prometheus.client.Histogram msgDelay)
    {
        this.sub = sub;
        this.unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Message.MAX_SIZE));
        this.quote = new Quote();
        this.trade = new Trade();
        this.histogram = histogram;
        this.assembler = new FragmentAssembler(this::onMessage);
        this.barrier = barrier;
        this.msgDelay = msgDelay;

        this.clock = new SystemNanoClock();
        this.epochClock = new OffsetEpochNanoClock();
        this.nowNs = clock.nanoTime();
        this.startTimeNs = nowNs + Config.warmUpDurationNs;
        this.endTimeNs = testDurationNs > 0 ? startTimeNs + testDurationNs : -1;
        this.resetIntervalNs = 60_000_000_000L;
        this.nextResetTimeNs = testDurationNs == 0 ? startTimeNs + resetIntervalNs: -1;
        this.reportIntervalNs = 5_000_000_000L;
        this.nextReportTimeNs = startTimeNs + reportIntervalNs;
    }

    private void onMessage(DirectBuffer buffer, int offset, int length, Header header)
    {
        buffer.getBytes(offset, unsafeBuffer, 0, length);

        int msgType = unsafeBuffer.getByte(0);
        switch (msgType)
        {
        case Message.QUOTE:
            this.quote.fromByteBuffer(unsafeBuffer.byteBuffer());
            nowNs = clock.nanoTime();
            if (this.onScheduleMeasure(nowNs))
            {
                histogram.recordValue(max(1, epochClock.nanoTime() - quote.receivedAt()));
                if (msgDelay != null)
                {
                    msgDelay.observe((double) max(1, epochClock.nanoTime() - quote.receivedAt()) / Constant.NANOS_PER_SECOND);
                }
            }
            if (this.onScheduleReset(nowNs))
            {
                histogram.reset();
            }
            if (this.onScheduleReport(nowNs))
            {
                reportPercentiles();
            }
            // LOG.info("quote: {}", quote.receivedAt());
            break;

        case Message.TRADE:
            this.trade.fromByteBuffer(unsafeBuffer.byteBuffer());
            nowNs = clock.nanoTime();
            if (this.onScheduleMeasure(nowNs))
            {
                histogram.recordValue(max(1, epochClock.nanoTime() - trade.receivedAt()));
                if (msgDelay != null)
                {
                    msgDelay.observe((double) max(1, epochClock.nanoTime() - trade.receivedAt()) / Constant.NANOS_PER_SECOND);
                }
            }
            if (this.onScheduleReset(nowNs))
            {
                histogram.reset();
            }
            if (this.onScheduleReport(nowNs))
            {
                reportPercentiles();
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
        if (endTimeNs != -1 && endTimeNs <= nowNs)
        {
            barrier.signal();
        }
        return fragmentReceived;
    }

    private void reportPercentiles()
    {
        LOG.info("latencies (us) - p50: {} p90: {} p95: {} p99: {}",
            histogram.getValueAtPercentile(50) / 1000,
            histogram.getValueAtPercentile(90) / 1000,
            histogram.getValueAtPercentile(95) / 1000,
            histogram.getValueAtPercentile(99) / 1000
        );
    }

    @Override
    public String roleName()
    {
        return "receiver";
    }

    private boolean onScheduleMeasure(long nowNs)
    {
        
        if (startTimeNs <= nowNs)
        {
            return true;
        }
        return false;
    }

    private boolean onScheduleReset(long nowNs)
    {
        if (nextResetTimeNs != -1 && nextResetTimeNs <= nowNs)
        {
            nextResetTimeNs += resetIntervalNs;
            return true;
        }
        return false;
    }

    private boolean onScheduleReport(long nowNs)
    {
        if (nextReportTimeNs <= nowNs)
        {
            nextReportTimeNs += reportIntervalNs;
            return true;
        }
        return false;
    }
}
