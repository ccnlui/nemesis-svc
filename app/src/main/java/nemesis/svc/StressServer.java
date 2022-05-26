package nemesis.svc;

import static nemesis.svc.Util.aeronIpcOrUdpChannel;
import static nemesis.svc.Util.closeIfNotNull;
import static nemesis.svc.Util.connectAeron;
import static nemesis.svc.Util.launchEmbeddedMediaDriverIfConfigured;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.CompositeAgent;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import nemesis.svc.agent.SendAgent;
import nemesis.svc.message.Quote;
import nemesis.svc.message.Trade;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "stress-server",
    description = "produce and publish quote + trade messages",
    usageHelpAutoWidth = true)
public class StressServer implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option(names = "--embedded-media-driver", defaultValue = "false",
        description = "launch with embedded media driver (default ${DEFAULT-VALUE})")
    boolean embeddedMediaDriver;

    @Option(names = "--aeron-dir", description = "directory name for aeron media driver")
    String aeronDir;

    @Option(names = "--pub-endpoint", description = "aeron udp transport endpoint to which messages are published <address:port>")
    String pubEndpoint;

    @Option(names = "--quote-interval", description = "set the interval for producing quotes")
    long quoteIntervalUs;

    @Option(names = "--trade-interval",description = "set the interval for producing trades")
    long tradeIntervalUs;

    private static final Logger LOG = LoggerFactory.getLogger(StressServer.class);

    @Override
    public Void call() throws Exception
    {
        mergeConfig();

        final MediaDriver mediaDriver = launchEmbeddedMediaDriverIfConfigured();
        final Aeron aeron = connectAeron(mediaDriver);

        final String outChannel = aeronIpcOrUdpChannel(Config.pubEndpoint);
        final int outStream = Config.exchangeDataStream;
        final Publication pub = aeron.addPublication(outChannel, outStream);
        LOG.info("out: {}:{}", outChannel, outStream);

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        Quote quote = new Quote();
        Trade trade = new Trade();
        quote.setFakeValues(ByteBuffer.allocateDirect(Quote.MAX_SIZE));
        trade.setFakeValues(ByteBuffer.allocateDirect(Trade.MAX_SIZE));
        final SendAgent sendQuotes = new SendAgent(pub, quote, Config.quoteIntervalUs * 1000L);
        final SendAgent sendTrades = new SendAgent(pub, trade, Config.tradeIntervalUs * 1000L);
        final AgentRunner agentRunner = new AgentRunner(
            Config.idleStrategy,
            Throwable::printStackTrace,
            null,
            new CompositeAgent(sendQuotes, sendTrades)
        );
        AgentRunner.startOnThread(agentRunner);

        barrier.await();
        closeIfNotNull(agentRunner);
        closeIfNotNull(aeron);
        closeIfNotNull(mediaDriver);
        return null;
    }

    private void mergeConfig()
    {
        if (this.embeddedMediaDriver)
        {
            Config.embeddedMediaDriver = this.embeddedMediaDriver;
        }
        if (this.aeronDir != null && !this.aeronDir.isEmpty())
        {
            Config.aeronDir = this.aeronDir;
        }
        if (this.pubEndpoint != null && !this.pubEndpoint.isEmpty())
        {
            Config.pubEndpoint = this.pubEndpoint;
        }
        if (this.quoteIntervalUs != 0)
        {
            Config.quoteIntervalUs = this.quoteIntervalUs;
        }
        if (this.tradeIntervalUs != 0)
        {
            Config.tradeIntervalUs = this.tradeIntervalUs;
        }
    }
}
