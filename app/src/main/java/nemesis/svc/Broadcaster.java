package nemesis.svc;

import static nemesis.svc.Util.aeronIpcOrUdpChannel;
import static nemesis.svc.Util.closeIfNotNull;
import static nemesis.svc.Util.connectAeron;
import static nemesis.svc.Util.launchEmbeddedMediaDriverIfConfigured;

import java.nio.file.Paths;
import java.util.concurrent.Callable;

import com.aitusoftware.babl.config.BablConfig;
import com.aitusoftware.babl.config.PropertiesLoader;
import com.aitusoftware.babl.websocket.BablServer;
import com.aitusoftware.babl.websocket.SessionContainers;

import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import nemesis.svc.agent.BablBroadcastAgent;
import nemesis.svc.agent.BroadcastAgent;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "broadcaster",
    usageHelpAutoWidth = true,
    description = "start websocket broadcast server")
public class Broadcaster implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option(names = "--embedded-media-driver", defaultValue = "false",
        description = "launch with embedded media driver (default ${DEFAULT-VALUE})")
    boolean embeddedMediaDriver;

    @Option(names = "--aeron-dir", description = "override directory name for embedded aeron media driver")
    String aeronDir;

    @Option(names = "--sub-endpoint", description = "aeron udp transport endpoint from which messages are subscribed <address:port>")
    String subEndpoint;

    @Option(names = "--websocket-lib", description = "websocket server library")
    String websocketLib;

    @Option(names = "--port", description = "websocket server port")
    int websocketPort;

    private static final Logger LOG = LoggerFactory.getLogger(Broadcaster.class);

    @Override
    public Void call() throws Exception
    {
        mergeConfig();

        final MediaDriver mediaDriver = launchEmbeddedMediaDriverIfConfigured();
        final Aeron aeron = connectAeron(mediaDriver);

        final String inChannel = aeronIpcOrUdpChannel(Config.subEndpoint);
        final int inStream = Config.websocketDataStream;
        final Subscription sub = aeron.addSubscription(inChannel, inStream);
        LOG.info("in: {}:{}", inChannel, inStream);

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        AgentRunner agentRunner = null;
        SessionContainers containers = null;
        switch (Config.websocketLib)
        {
            case "babl" ->
            {
                LOG.info("babl config path: {}", Config.bablConfigPath);
                final BablConfig config = PropertiesLoader.configure(Paths.get(Config.bablConfigPath));
                final BablStreamServer bablStreamServer = new BablStreamServer();
                config.applicationConfig().application(bablStreamServer);  // this is needed to register broadcastSource
                config.proxyConfig().mediaDriverDir(aeron.context().aeronDirectoryName());  // always reuse media driver

                final BablBroadcastAgent bablBroadcastAgent = new BablBroadcastAgent(sub, bablStreamServer);
                config.applicationConfig().additionalWork(bablBroadcastAgent);

                containers = BablServer.launch(config);
                containers.start();
                bablStreamServer.createBroadcastTopic();
            }
            case "java-websocket" ->
            {
                final StreamServer streamServer = new StreamServer(Config.websocketPort);
                final BroadcastAgent broadcastAgent = new BroadcastAgent(sub, streamServer);
                agentRunner = new AgentRunner(
                    Config.idleStrategy,
                    Throwable::printStackTrace,
                    null,
                    broadcastAgent
                );
                streamServer.start();
                AgentRunner.startOnThread(agentRunner);
            }
        }
        barrier.await();
        closeIfNotNull(agentRunner);
        closeIfNotNull(containers);
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
        if (this.subEndpoint != null && !this.subEndpoint.isEmpty())
        {
            Config.subEndpoint = this.subEndpoint;
        }
        if (this.websocketPort != 0)
        {
            Config.websocketPort = this.websocketPort;
        }
        if (this.websocketLib != null && !this.websocketLib.isEmpty())
        {
            Config.websocketLib = this.websocketLib;
        }
    }
}
