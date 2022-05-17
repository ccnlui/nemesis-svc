package nemesis.svc;

import java.util.concurrent.Callable;

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
import nemesis.svc.agent.MarshalAgent;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "marshaller",
    usageHelpAutoWidth = true,
    description = "marshall nemesis trades and quotes messages")
public class Marshaller implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option(names = "--embedded-media-driver", defaultValue = "false",
        description = "launch with embedded media driver (default ${DEFAULT-VALUE})")
    boolean embeddedMediaDriver;

    @Option(names = "--aeron-dir", description = "override directory name for embedded aeron media driver")
    String aeronDir;

    @Option(names = "--pub-endpoint", defaultValue = "",
        description = "aeron udp transport endpoint to which messages are published in address:port format (default: \"${DEFAULT-VALUE}\")")
    String pubEndpoint;

    private static final Logger LOG = LoggerFactory.getLogger(Marshaller.class);

    @Override
    public Void call() throws Exception
    {
        final String inChannel = "aeron:ipc";
        final int inStream = 11;
        final String outChannel = aeronIpcOrUdpChannel(pubEndpoint);
        final int outStream = 12;
        final IdleStrategy idleStrategy = new BusySpinIdleStrategy();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        final MediaDriver mediaDriver = launchEmbeddedMediaDriverIfConfigured();
        String aeronDirName = mediaDriver == null ? null : mediaDriver.aeronDirectoryName();
        final Aeron aeron = connectAeron(aeronDirName);

        // construct publication and subscription
        final Subscription sub = aeron.addSubscription(inChannel, inStream);
        final Publication pub = aeron.addPublication(outChannel, outStream);

        // construct the agents
        final MarshalAgent marshalAgent = new MarshalAgent(sub, pub);
        final AgentRunner agentRunner = new AgentRunner(
            idleStrategy,
            Throwable::printStackTrace,
            null,
            marshalAgent
        );

        LOG.info("marshaller: in: {}:{}", inChannel, inStream);
        LOG.info("marshaller: out: {}:{}", outChannel, outStream);
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

    private String aeronIpcOrUdpChannel(String endpoint)
    {
        if (endpoint == null || endpoint.isEmpty())
        {
            return "aeron:ipc";
        }
        else
        {
            return "aeron:udp?endpoint=" + endpoint + "|mtu=1408";
        }
    }

    private void closeIfNotNull(final AutoCloseable closeable) throws Exception
    {
        if (closeable != null)
            closeable.close();
    }
}
