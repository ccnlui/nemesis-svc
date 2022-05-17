package nemesis.svc;

import java.nio.file.Paths;
import java.util.concurrent.Callable;

import com.aitusoftware.babl.config.BablConfig;
import com.aitusoftware.babl.config.PropertiesLoader;
import com.aitusoftware.babl.websocket.BablServer;
import com.aitusoftware.babl.websocket.SessionContainers;

import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
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

    @Option(names = "--sub-endpoint", defaultValue = "",
        description = "aeron udp transport endpoint from which messages are subscribed in address:port format (default: \"${DEFAULT-VALUE}\")")
    String subEndpoint;

    @Option(names = "--port", defaultValue = "${PORT:-8080}",
        description = "websocket server port (default: ${DEFAULT-VALUE})")
    int port;

    @Option(names = "--websocket-lib", defaultValue = "${WEBSOCKET_LIB:-babl}",
        description = "websocket server library (default: ${DEFAULT-VALUE})")
    String websocketLib;

    private static final Logger LOG = LoggerFactory.getLogger(Broadcaster.class);

    @Override
    public Void call() throws Exception
    {
        final String inChannel = aeronIpcOrUdpChannel(subEndpoint);
        final int inStream = 12;
        final IdleStrategy idleStrategy = new BusySpinIdleStrategy();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        final MediaDriver mediaDriver = launchEmbeddedMediaDriverIfConfigured();
        String aeronDirName = mediaDriver == null ? null : mediaDriver.aeronDirectoryName();
        final Aeron aeron = connectAeron(aeronDirName);
        
        // construct publication and subscription
        final Subscription sub = aeron.addSubscription(inChannel, inStream);

        
        AgentRunner agentRunner = null;
        SessionContainers containers = null;
        switch (websocketLib)
        {
            case "babl" ->
            {
                // construct babl server
                final String configPath = System.getProperty("user.dir") + "/build/resources/main/babl-default.properties";
                // final String configPath = "/home/calvin/source/java/nemesis-svc/babl-performance.properties";
                LOG.info("configPath: {}", configPath);
                final BablConfig config = PropertiesLoader.configure(Paths.get(configPath));
                final BablStreamServer bablStreamServer = new BablStreamServer();
                config.applicationConfig().application(bablStreamServer);  // this is needed to register broadcastSource

                // construct the agents
                final BablBroadcastAgent bablBroadcastAgent = new BablBroadcastAgent(sub, bablStreamServer);
                config.applicationConfig().additionalWork(bablBroadcastAgent);

                LOG.info("babl broadcaster: in: {}:{}", inChannel, inStream);
                containers = BablServer.launch(config);
                containers.start();
                bablStreamServer.createBroadcastTopic();
            }
                
            case "java-websocket" ->
            {
                // construct java-websocket server
                final StreamServer streamServer = new StreamServer(port);

                // construct the agents
                final BroadcastAgent broadcastAgent = new BroadcastAgent(sub, streamServer);
                agentRunner = new AgentRunner(
                    idleStrategy,
                    Throwable::printStackTrace,
                    null,
                    broadcastAgent
                );

                LOG.info("java-websocket broadcaster: in channel:stream: {}:{}", inChannel, inStream);
                streamServer.start();
                AgentRunner.startOnThread(agentRunner);
            }
        }

        // wait for the shutdown signal
        barrier.await();

        // close the resources
        closeIfNotNull(agentRunner);
        closeIfNotNull(containers);
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
