package nemesis.svc;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
(
    name = "stress-client",
    description = "subscribe to quote + trade messages",
    usageHelpAutoWidth = true
)
public class StressClient implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    private static final Logger LOG = LoggerFactory.getLogger(StressClient.class);

    @Override
    public Void call() throws Exception
    {
        final String channel = "aeron:udp?endpoint=127.0.0.1:2000|mtu=1408";
        final int stream = 10;
        final IdleStrategy idleStrategyReceive = new BusySpinIdleStrategy();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

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

        // construct the subscription
        final Subscription sub = aeron.addSubscription(channel, stream);

        // construct the agents
        Histogram histogram = new Histogram(TimeUnit.MINUTES.toNanos(1), 3);
        final ReceiveAgent receiveAgent = new ReceiveAgent(sub, histogram, barrier);
        final AgentRunner agentRunner = new AgentRunner(
            idleStrategyReceive,
            Throwable::printStackTrace,
            null,
            receiveAgent
        );

        LOG.info("starting");
        AgentRunner.startOnThread(agentRunner);

        // wait for the shutdown signal
        barrier.await();

        // close the resources
        agentRunner.close();
        aeron.close();
        mediaDriver.close();

        LOG.info("---------- stressClientInDelay (us) ----------");
        histogram.outputPercentileDistribution(System.out, 1000.0);  // output in us

        return null;
    }
}
