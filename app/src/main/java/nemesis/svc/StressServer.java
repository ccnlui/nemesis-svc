package nemesis.svc;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.CompositeAgent;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import nemesis.svc.agent.SendAgent;
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

    @Option(names = "--embedded-media-driver", defaultValue = "false",
        description = "launch with embedded media driver (default ${DEFAULT-VALUE})")
    boolean embeddedMediaDriver;

    @Option(names = "--aeron-dir", description = "directory name for aeron media driver")
    String aeronDir;

    @Option(names = "--pub-endpoint",
        defaultValue = "${PUB_ENDPOINT:-127.0.0.1:2000}",
        description = "aeron udp transport endpoint to which messages are published in address:port format (default: \"${DEFAULT-VALUE}\")")
    String pubEndpoint;

    private static final Logger LOG = LoggerFactory.getLogger(StressServer.class);

    @Override
    public Void call() throws Exception
    {
        final String channel = "aeron:udp?endpoint=" + pubEndpoint + "|mtu=1408";
        final int stream = 10;
        final IdleStrategy idleStrategySend = new BusySpinIdleStrategy();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        final MediaDriver mediaDriver = launchEmbeddedMediaDriverIfConfigured();
        String aeronDirName = mediaDriver == null ? null : mediaDriver.aeronDirectoryName();
        final Aeron aeron = connectAeron(aeronDirName);

        // construct the publication
        final Publication pub = aeron.addPublication(channel, stream);

        // construct the agents
        Quote quote = new Quote();
        Trade trade = new Trade();
        quote.setFakeValues(ByteBuffer.allocateDirect(Quote.MAX_SIZE));
        trade.setFakeValues(ByteBuffer.allocateDirect(Trade.MAX_SIZE));

        final SendAgent sendQuotes = new SendAgent(pub, quote, quoteIntervalUs * 1000L);
        final SendAgent sendTrades = new SendAgent(pub, trade, tradeIntervalUs * 1000L);
        final AgentRunner agentRunner = new AgentRunner(
            idleStrategySend,
            Throwable::printStackTrace,
            null,
            new CompositeAgent(sendQuotes, sendTrades)
        );

        LOG.info("stress server: out: {}:{}", channel, stream);
        AgentRunner.startOnThread(agentRunner);

        // wait for the shutdown signal
        barrier.await();

        // close the resources
        closeIfNotNull(agentRunner);
        closeIfNotNull(aeron);
        closeIfNotNull(mediaDriver);

        return null;
    }

    private MediaDriver launchEmbeddedMediaDriverIfConfigured()
    {
        if (embeddedMediaDriver)
        {
            MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .threadingMode(ThreadingMode.DEDICATED)
                .conductorIdleStrategy(new BusySpinIdleStrategy())
                .senderIdleStrategy(new NoOpIdleStrategy())
                .receiverIdleStrategy(new NoOpIdleStrategy())
                .dirDeleteOnShutdown(true);
            if (aeronDir != null)
                mediaDriverCtx = mediaDriverCtx.aeronDirectoryName(aeronDir);
            MediaDriver md = MediaDriver.launchEmbedded(mediaDriverCtx);

            LOG.info(mediaDriverCtx.toString());
            return md;
        }
        return null;
    }

    private Aeron connectAeron(String aeronDirName)
    {
        Aeron.Context aeronCtx = new Aeron.Context()
            .idleStrategy(new NoOpIdleStrategy());
        if (aeronDirName != null)
            aeronCtx = aeronCtx.aeronDirectoryName(aeronDirName);
        else if (aeronDir != null)
            aeronCtx = aeronCtx.aeronDirectoryName(aeronDir);
        LOG.info(aeronCtx.toString());

        final Aeron aeron = Aeron.connect(aeronCtx);
        return aeron;
    }

    private void closeIfNotNull(final AutoCloseable closeable) throws Exception
    {
        if (closeable != null)
            closeable.close();
    }
}
