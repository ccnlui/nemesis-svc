package nemesis.svc;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
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

    @Option(names = "--sub-endpoint", defaultValue = "${SUB_ENDPOINT:-127.0.0.1:2000}",
        description = "aeron udp transport endpoint from which messages are subscribed in address:port format (default: \"${DEFAULT-VALUE}\")")
    String subEndpoint;

    @Option(names = "--aeron-dir", description = "override directory name for embedded aeron media driver")
    String aeronDir;

    private static final Logger LOG = LoggerFactory.getLogger(Connector.class);

    @Override
    public Void call() throws Exception
    {
        final String inChannel = "aeron:udp?endpoint=" + subEndpoint + "|mtu=1408";
        final String outChannel = "aeron:ipc";
        final int inStream = 10;
        final int outStream = 11;
        final IdleStrategy idleStrategy = new BusySpinIdleStrategy();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        final MediaDriver mediaDriver = launchEmbeddedMediaDriverIfConfigured();
        String defaultAeronDirName = mediaDriver == null ? null : mediaDriver.aeronDirectoryName();
        final Aeron aeron = connectAeron(defaultAeronDirName);

        // construct publication and subscription
        final Subscription sub = aeron.addSubscription(inChannel, inStream);
        final Publication pub = aeron.addPublication(outChannel, outStream);

        // construct the agents
        Histogram histogram = new Histogram(TimeUnit.MINUTES.toNanos(1), 3);
        final PipeAgent pipeAgent = new PipeAgent(sub, pub);
        final AgentRunner agentRunner = new AgentRunner(
            idleStrategy,
            Throwable::printStackTrace,
            null,
            pipeAgent
        );

        LOG.info("connector: in: {}:{}", inChannel, inStream);
        LOG.info("connector: out: {}:{}", outChannel, outStream);
        AgentRunner.startOnThread(agentRunner);

        // wait for the shutdown signal
        barrier.await();

        // close the resources
        closeIfNotNull(agentRunner);
        closeIfNotNull(aeron);
        closeIfNotNull(mediaDriver);

        LOG.info("---------- connectorInDelay (us) ----------");
        histogram.outputPercentileDistribution(System.out, 1000.0);  // output in us

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

    private Aeron connectAeron(String defaultAeronDirName)
    {
        Aeron.Context aeronCtx = new Aeron.Context()
            .idleStrategy(new NoOpIdleStrategy());
        if (defaultAeronDirName != null)
            aeronCtx = aeronCtx.aeronDirectoryName(defaultAeronDirName);
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
