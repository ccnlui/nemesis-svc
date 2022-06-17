package nemesis.svc.nanoservice;

import static nemesis.svc.nanoservice.Util.aeronIpcOrUdpChannel;
import static nemesis.svc.nanoservice.Util.closeIfNotNull;
import static nemesis.svc.nanoservice.Util.connectAeron;
import static nemesis.svc.nanoservice.Util.launchEmbeddedMediaDriverIfConfigured;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.prometheus.client.exporter.HTTPServer;

public class StressClient
{
    private static final Logger LOG = LoggerFactory.getLogger(StressClient.class);

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final Subscription sub;
    private final Histogram histogram;
    private final long testDurationNs;
    private io.prometheus.client.Histogram msgDelay;

    public StressClient()
    {
        this.mediaDriver = launchEmbeddedMediaDriverIfConfigured();
        this.aeron = connectAeron(mediaDriver);

        final String inChannel = aeronIpcOrUdpChannel(Config.subEndpoint);
        final int inStream = Config.exchangeDataStream;
        this.sub = aeron.addSubscription(inChannel, inStream);
        LOG.info("in: {}:{}", inChannel, inStream);

        this.histogram = new Histogram(TimeUnit.MINUTES.toNanos(1), 3);
        this.testDurationNs = Config.testDurationNs;
        LOG.info("Benchmark duration: {}sec after warmup {}sec...",
            this.testDurationNs / TimeUnit.SECONDS.toNanos(1),
            Config.warmUpDurationNs / TimeUnit.SECONDS.toNanos(1)
        );
    }

    public void run() throws Exception
    {
        final HTTPServer metricsServer = Config.enableMetrics ? startMetricServer(Config.metricsPort) : null;
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final ReceiveAgent receiveAgent = new ReceiveAgent(sub, histogram, barrier, testDurationNs, msgDelay);
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
        closeIfNotNull(metricsServer);
        histogram.outputPercentileDistribution(System.out, 1000.0);  // output in us
    }

    private HTTPServer startMetricServer(int port) throws IOException
    {
        this.msgDelay = io.prometheus.client.Histogram.build()
            .namespace("stress_client")
            .name("message_delay_seconds")
            .help("udp multicast message delay in seconds")
            .buckets(0.001, 0.005, 0.02, 0.05, 0.1, 0.5, 1, 5, 10)
            .register();
        return new HTTPServer.Builder().withPort(port).build();
    }
}
