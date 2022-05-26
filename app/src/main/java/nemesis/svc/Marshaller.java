package nemesis.svc;

import static nemesis.svc.Util.aeronIpcOrUdpChannel;
import static nemesis.svc.Util.closeIfNotNull;
import static nemesis.svc.Util.connectAeron;
import static nemesis.svc.Util.launchEmbeddedMediaDriverIfConfigured;

import java.util.concurrent.Callable;

import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import nemesis.svc.agent.MarshalAgent;
import nemesis.svc.message.Message;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(
    name = "marshaller",
    usageHelpAutoWidth = true,
    description = "marshall nemesis trades and quotes messages")
public class Marshaller implements Callable<Void>
{
    @Spec CommandSpec spec;

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

    @Option(names = "--format", description = "message format: json or msgpack")
    Message.Format format;

    private static final Logger LOG = LoggerFactory.getLogger(Marshaller.class);

    @Override
    public Void call() throws Exception
    {
        mergeConfig();

        final MediaDriver mediaDriver = launchEmbeddedMediaDriverIfConfigured();
        final Aeron aeron = connectAeron(mediaDriver);

        final String inChannel = aeronIpcOrUdpChannel(Config.subEndpoint);
        final String outChannel = aeronIpcOrUdpChannel(Config.pubEndpoint);
        final int inStream = Config.pipedDataStream;
        final int outStream = Config.websocketDataStream;
        final Publication pub = aeron.addPublication(outChannel, outStream);
        final Subscription sub = aeron.addSubscription(inChannel, inStream);
        LOG.info("in: {}:{}", inChannel, inStream);
        LOG.info("out: {}:{}", outChannel, outStream);

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final MarshalAgent marshalAgent = new MarshalAgent(sub, pub, Config.messageFormat);
        final AgentRunner agentRunner = new AgentRunner(
            Config.idleStrategy,
            Throwable::printStackTrace,
            null,
            marshalAgent
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
        if (this.subEndpoint != null && !this.subEndpoint.isEmpty())
        {
            Config.subEndpoint = this.subEndpoint;
        }
        if (this.format != null)
        {
            Config.messageFormat = this.format;
        }
    }
}
