package nemesis.svc.nanoservice;

import static nemesis.svc.nanoservice.Util.aeronIpcOrUdpChannel;
import static nemesis.svc.nanoservice.Util.closeIfNotNull;
import static nemesis.svc.nanoservice.Util.connectAeron;
import static nemesis.svc.nanoservice.Util.launchEmbeddedMediaDriverIfConfigured;

import java.nio.ByteBuffer;

import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.CompositeAgent;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import nemesis.svc.message.Quote;
import nemesis.svc.message.Trade;

public class StressServer
{
    private static final Logger LOG = LoggerFactory.getLogger(StressServer.class);

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final Publication pub;

    public StressServer()
    {
        this.mediaDriver = launchEmbeddedMediaDriverIfConfigured();
        this.aeron = connectAeron(mediaDriver);

        final String outChannel = aeronIpcOrUdpChannel(Config.pubEndpoint);
        final int outStream = Config.exchangeDataStream;
        this.pub = aeron.addPublication(outChannel, outStream);
        LOG.info("out: {}:{}", outChannel, outStream);
    }

    public void run() throws Exception
    {
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        Quote quote = new Quote();
        Trade trade = new Trade();
        quote.setFakeValues(ByteBuffer.allocateDirect(Quote.MAX_SIZE));
        trade.setFakeValues(ByteBuffer.allocateDirect(Trade.MAX_SIZE));
        final SendAgent sendQuotes = new SendAgent(pub, quote, Config.quoteIntervalUs * 1000L);
        final SendAgent sendTrades = new SendAgent(pub, trade, Config.tradeIntervalUs * 1000L);
        final AgentRunner agentRunner = new AgentRunner(
            Config.idleStrategy,
            Throwable::printStackTrace,
            null,
            new CompositeAgent(sendQuotes, sendTrades)
        );
        AgentRunner.startOnThread(agentRunner);
        barrier.await();
        closeIfNotNull(agentRunner);
        closeIfNotNull(aeron);
        closeIfNotNull(mediaDriver);
    }    
}
