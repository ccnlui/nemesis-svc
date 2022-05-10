package nemesis.svc;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Callable;

import org.HdrHistogram.Histogram;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import nemesis.svc.message.cqs.TransmissionBlock;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
(
    name = "subscriber",
    description = "subscribe to marketdata via aeron udp messages",
    usageHelpAutoWidth = true
)
public class Subscriber implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option(names = {"-b", "--bench"}, description = "measure inbound delay for 20 sec")
    boolean bench;

    private static final Logger LOG = LoggerFactory.getLogger(Subscriber.class);

    final long WARMUP_TIME_MSEC = 10_000;
    final long RUN_TIME_MSEC    = 20_000;

    @Override
    public Void call() throws Exception
    {
        final UnsafeBuffer usb = new UnsafeBuffer(ByteBuffer.allocateDirect(TransmissionBlock.MAX_SIZE));
        final TransmissionBlock block = new TransmissionBlock();

        final String channel = "aeron:udp?endpoint=127.0.0.1:2000|mtu=1408";
        final int stream = 10;
        final IdleStrategy idleStrategyReceive = new BusySpinIdleStrategy();

        long startTime  = System.currentTimeMillis();
        Histogram scbInDelay = new Histogram(60_000_000_000L, 3);

        // construct media driver, clean up media driver folder on start/stop
        final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new BusySpinIdleStrategy())
            .dirDeleteOnShutdown(true);
        final MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaDriverCtx);

        // construct aeron, point at the media driver's folder
        final Aeron.Context aeronCtx = new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName());
        final Aeron aeron = Aeron.connect(aeronCtx);

        LOG.info("Dir: {}", mediaDriver.aeronDirectoryName());

        final Subscription sub = aeron.addSubscription(channel, stream);
        final FragmentHandler handler =
            (buffer, offset, length, header) ->
            {
                buffer.getBytes(offset, usb, 0, length);
                block.fromByteBuffer(usb.byteBuffer());
                // block.parseHeader();
                if (bench && (System.currentTimeMillis() - startTime > WARMUP_TIME_MSEC))
                {
                    scbInDelay.recordValue(nowNano() - block.sipBlockTimestamp());
                }
            };

        while (true)
        {
            final int fragmentsRead = sub.poll(handler, 10);
            if (bench && System.currentTimeMillis() - startTime > WARMUP_TIME_MSEC + RUN_TIME_MSEC)
            {
                LOG.info("---------- scbInDelay (us) ----------");
                scbInDelay.outputPercentileDistribution(System.out, 1000.0);  // output in us
                break;
            }
            idleStrategyReceive.idle(fragmentsRead);
        }

        // clean up
        sub.close();
        aeron.close();
        mediaDriver.close();

        return null;
    }

    private long nowNano()
    {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }
}
