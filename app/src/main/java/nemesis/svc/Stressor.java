package nemesis.svc;

import java.util.concurrent.Callable;

import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.CompositeAgent;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import nemesis.svc.message.Quote;
import nemesis.svc.message.Trade;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
(
    name = "stressor",
    description = "produce and publish quote + trade messages",
    usageHelpAutoWidth = true
)
public class Stressor implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    private static final Logger LOG = LoggerFactory.getLogger(Stressor.class);

    @Override
    public Void call() throws Exception
    {
        final String channel = "aeron:udp?endpoint=127.0.0.1:2000|mtu=1408";
        final int stream = 10;
        final IdleStrategy idleStrategySend = new BusySpinIdleStrategy();
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

        // construct the subs and pubs
        final Publication pub = aeron.addPublication(channel, stream);

        // construct the agents
        Quote quote = new Quote();
        Trade trade = new Trade();

        final SendAgent sendQuotes = new SendAgent(pub, quote, 100_000L);
        final SendAgent sendTrades = new SendAgent(pub, trade, 200_000L);
        final AgentRunner agentRunner = new AgentRunner(
            idleStrategySend,
            Throwable::printStackTrace,
            null,
            new CompositeAgent(sendQuotes, sendTrades)
        );

        LOG.info("starting");
        AgentRunner.startOnThread(agentRunner);

        // wait for the shutdown signal
        barrier.await();

        // close the resources
        agentRunner.close();
        aeron.close();
        mediaDriver.close();

        return null;
    }
}
