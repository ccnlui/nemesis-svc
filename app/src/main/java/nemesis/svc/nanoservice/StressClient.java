package nemesis.svc.nanoservice;

import static nemesis.svc.nanoservice.Util.aeronIpcOrUdpChannel;
import static nemesis.svc.nanoservice.Util.closeIfNotNull;
import static nemesis.svc.nanoservice.Util.connectAeron;
import static nemesis.svc.nanoservice.Util.launchEmbeddedMediaDriverIfConfigured;

import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import nemesis.svc.agent.ReceiveAgent;

public class StressClient
{
    private static final Logger LOG = LoggerFactory.getLogger(StressClient.class);

    public void run() throws Exception
    {
        final MediaDriver mediaDriver = launchEmbeddedMediaDriverIfConfigured();
        final Aeron aeron = connectAeron(mediaDriver);

        final String inChannel = aeronIpcOrUdpChannel(Config.subEndpoint);
        final int inStream = Config.exchangeDataStream;
        final Subscription sub = aeron.addSubscription(inChannel, inStream);
        LOG.info("in: {}:{}", inChannel, inStream);

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        Histogram histogram = new Histogram(TimeUnit.MINUTES.toNanos(1), 3);
        final ReceiveAgent receiveAgent = new ReceiveAgent(sub, histogram, barrier);
        final AgentRunner agentRunner = new AgentRunner(
            Config.idleStrategy,
            Throwable::printStackTrace,
            null,
            receiveAgent
        );
        AgentRunner.startOnThread(agentRunner);
        barrier.await();
        closeIfNotNull(agentRunner);
        closeIfNotNull(aeron);
        closeIfNotNull(mediaDriver);

        LOG.info("---------- stressClientInDelay (us) ----------");
        histogram.outputPercentileDistribution(System.out, 1000.0);  // output in us
    }    
}
