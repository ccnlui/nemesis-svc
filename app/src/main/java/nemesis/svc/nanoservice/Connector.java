package nemesis.svc.nanoservice;

import static nemesis.svc.nanoservice.Util.aeronIpcOrUdpChannel;
import static nemesis.svc.nanoservice.Util.closeIfNotNull;
import static nemesis.svc.nanoservice.Util.connectAeron;
import static nemesis.svc.nanoservice.Util.launchEmbeddedMediaDriverIfConfigured;

import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;

public class Connector
{
    private static final Logger LOG = LoggerFactory.getLogger(Connector.class);

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final Publication pub;
    private final Subscription sub;

    public Connector()
    {
        this.mediaDriver = launchEmbeddedMediaDriverIfConfigured();
        this.aeron = connectAeron(mediaDriver);

        final String inChannel = aeronIpcOrUdpChannel(Config.subEndpoint);
        final String outChannel = aeronIpcOrUdpChannel(Config.pubEndpoint);
        final int inStream = Config.exchangeDataStream;
        final int outStream = Config.pipedDataStream;
        this.pub = aeron.addPublication(outChannel, outStream);
        this.sub = aeron.addSubscription(inChannel, inStream);
        LOG.info("in: {}:{}", inChannel, inStream);
        LOG.info("out: {}:{}", outChannel, outStream);
    }

    public void run() throws Exception
    {
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
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
    }
}
