package nemesis.svc.nanoservice;

import static nemesis.svc.nanoservice.Util.aeronIpcOrUdpChannel;
import static nemesis.svc.nanoservice.Util.closeIfNotNull;
import static nemesis.svc.nanoservice.Util.connectAeron;
import static nemesis.svc.nanoservice.Util.launchEmbeddedMediaDriverIfConfigured;

import java.nio.file.Paths;

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

public class Broadcaster
{
    private static final Logger LOG = LoggerFactory.getLogger(Broadcaster.class);

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final Subscription sub;

    public Broadcaster()
    {
        this.mediaDriver = launchEmbeddedMediaDriverIfConfigured();
        this.aeron = connectAeron(this.mediaDriver);

        final String inChannel = aeronIpcOrUdpChannel(Config.subEndpoint);
        final int inStream = Config.websocketDataStream;
        this.sub = aeron.addSubscription(inChannel, inStream);
        LOG.info("in: {}:{}", inChannel, inStream);
    }

    public void run() throws Exception
    {
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
                final JwsStreamServer streamServer = new JwsStreamServer(Config.websocketPort);
                final JwsBroadcastAgent broadcastAgent = new JwsBroadcastAgent(sub, streamServer);
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
    }
}
