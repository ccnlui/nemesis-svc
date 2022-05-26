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
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import nemesis.svc.agent.PipeAgent;

public class Connector
{
    private static final Logger LOG = LoggerFactory.getLogger(Connector.class);

    public void run() throws Exception
    {
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
    }
}
