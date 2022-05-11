package nemesis.svc;

import java.nio.ByteBuffer;
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
    name = "stress-server",
    description = "produce and publish quote + trade messages",
    usageHelpAutoWidth = true
)
public class StressServer implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option(names = "--quote-interval", defaultValue = "100",
        description = "set the interval for producing quotes (default ${DEFAULT-VALUE}us)")
    long quoteIntervalUs;

    @Option(names = "--trade-interval", defaultValue = "200",
        description = "set the interval for producing trades (default ${DEFAULT-VALUE}us)")
    long tradeIntervalUs;

    private static final Logger LOG = LoggerFactory.getLogger(StressServer.class);

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

        // construct the publication
        final Publication pub = aeron.addPublication(channel, stream);

        // construct the agents
        Quote quote = new Quote(ByteBuffer.allocateDirect(Quote.MAX_SIZE));
        Trade trade = new Trade(ByteBuffer.allocateDirect(Trade.MAX_SIZE));

        final SendAgent sendQuotes = new SendAgent(pub, quote, quoteIntervalUs * 1000L);
        final SendAgent sendTrades = new SendAgent(pub, trade, tradeIntervalUs * 1000L);
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
