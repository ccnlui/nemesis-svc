package nemesis.svc;

import static nemesis.svc.Util.aeronIpcOrUdpChannel;
import static nemesis.svc.Util.closeIfNotNull;
import static nemesis.svc.Util.connectAeron;
import static nemesis.svc.Util.launchEmbeddedMediaDriverIfConfigured;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import nemesis.svc.agent.PipeAgent;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "connector",
    usageHelpAutoWidth = true,
    description = "subscribe to quote + trade messages and pipe to marshaller")
public class Connector implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option(names = "--embedded-media-driver", defaultValue = "false",
        description = "launch with embedded media driver (default ${DEFAULT-VALUE})")
    boolean embeddedMediaDriver;

    @Option(names = "--aeron-dir", description = "override directory name for embedded aeron media driver")
    String aeronDir;

    @Option(names = "--pub-endpoint", description = "aeron udp transport endpoint to which messages are published <address:port>")
    String pubEndpoint;

    @Option(names = "--sub-endpoint", description = "aeron udp transport endpoint from which messages are subscribed <address:port>")
    String subEndpoint;

    private static final Logger LOG = LoggerFactory.getLogger(Connector.class);

    @Override
    public Void call() throws Exception
    {
        mergeConfig();

        final MediaDriver mediaDriver = launchEmbeddedMediaDriverIfConfigured();
        final Aeron aeron = connectAeron(mediaDriver);

        final String inChannel = aeronIpcOrUdpChannel(Config.subEndpoint);
        final String outChannel = aeronIpcOrUdpChannel(Config.pubEndpoint);
        final int inStream = Config.exchangeDataStream;
        final int outStream = Config.pipedDataStream;
        final Publication pub = aeron.addPublication(outChannel, outStream);
        final Subscription sub = aeron.addSubscription(inChannel, inStream);
        LOG.info("in: {}:{}", inChannel, inStream);
        LOG.info("out: {}:{}", outChannel, outStream);

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        Histogram histogram = new Histogram(TimeUnit.MINUTES.toNanos(1), 3);
        final PipeAgent pipeAgent = new PipeAgent(sub, pub);
        final AgentRunner agentRunner = new AgentRunner(
            Config.idleStrategy,
            Throwable::printStackTrace,
            null,
            pipeAgent
        );

        AgentRunner.startOnThread(agentRunner);
        barrier.await();
        closeIfNotNull(agentRunner);
        closeIfNotNull(aeron);
        closeIfNotNull(mediaDriver);

        LOG.info("---------- connectorInDelay (us) ----------");
        histogram.outputPercentileDistribution(System.out, 1000.0);  // output in us
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
        if (this.subEndpoint != null && !this.subEndpoint.isEmpty())
        {
            Config.subEndpoint = this.subEndpoint;
        }
    }
}
