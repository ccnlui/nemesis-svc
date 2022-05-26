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
import nemesis.svc.agent.MarshalAgent;

public class Marshaller
{
    private static final Logger LOG = LoggerFactory.getLogger(Marshaller.class);

    public void run() throws Exception
    {
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
    }    
}
