package nemesis.svc.nanoservice;

import static nemesis.svc.nanoservice.Util.aeronIpcOrUdpChannel;
import static nemesis.svc.nanoservice.Util.closeIfNotNull;
import static nemesis.svc.nanoservice.Util.connectAeron;
import static nemesis.svc.nanoservice.Util.launchEmbeddedMediaDriverIfConfigured;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import org.agrona.BufferUtil;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import nemesis.svc.multicast.UdpTransportPoller;

public class NyseServer
{
    private static final Logger LOG = LoggerFactory.getLogger(NyseServer.class);

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final Publication pub;
    private final UdpTransportPoller udpPoller;

    public NyseServer()
    {
        this.mediaDriver = launchEmbeddedMediaDriverIfConfigured();
        this.aeron = connectAeron(mediaDriver);

        final String outChannel = aeronIpcOrUdpChannel(Config.pubEndpoint);
        final int outStream = Config.exchangeDataStream;
        this.pub = aeron.addPublication(outChannel, outStream);
        LOG.info("out: {}:{}", outChannel, outStream);

        final ByteBuffer inBuf = BufferUtil.allocateDirectAligned(Config.maxUdpMessageSize, 64);
        this.udpPoller = new UdpTransportPoller(inBuf, Throwable::printStackTrace);
        subscribeToMarketdata(udpPoller);
    }

    public void run() throws Exception
    {
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final NyseForwardAgent forwardAgent = new NyseForwardAgent(pub, udpPoller);
        final AgentRunner agentRunner = new AgentRunner(
            Config.idleStrategy,
            Throwable::printStackTrace,
            null,
            forwardAgent
        );
        AgentRunner.startOnThread(agentRunner);
        barrier.await();
        closeIfNotNull(agentRunner);
        closeIfNotNull(udpPoller);
        closeIfNotNull(pub);
        closeIfNotNull(aeron);
        closeIfNotNull(mediaDriver);
    }

    private void subscribeToMarketdata(UdpTransportPoller udpPoller)
    {
        try
        {
            // Tape A - trades
            multicastSubscribe(Config.networkInterface, "224.0.89.0", 40000, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.1", 40001, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.2", 40002, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.3", 40003, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.4", 40004, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.5", 40005, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.6", 40006, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.7", 40007, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.8", 40008, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.9", 40009, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.10", 40010, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.11", 40011, udpPoller);

            // Tape B - trades
            multicastSubscribe(Config.networkInterface, "224.0.89.32", 40000, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.33", 40001, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.34", 40002, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.35", 40003, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.36", 40004, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.37", 40005, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.38", 40006, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.39", 40007, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.40", 40008, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.41", 40009, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.42", 40010, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.89.43", 40011, udpPoller);

            // Tape A - quotes
            multicastSubscribe(Config.networkInterface, "224.0.90.0", 40000, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.1", 40001, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.2", 40002, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.3", 40003, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.4", 40004, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.5", 40005, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.6", 40006, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.7", 40007, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.8", 40008, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.9", 40009, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.10", 40010, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.11", 40011, udpPoller);

            // Tape B - quotes
            multicastSubscribe(Config.networkInterface, "224.0.90.32", 40000, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.33", 40001, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.34", 40002, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.35", 40003, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.36", 40004, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.37", 40005, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.38", 40006, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.39", 40007, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.40", 40008, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.41", 40009, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.42", 40010, udpPoller);
            multicastSubscribe(Config.networkInterface, "224.0.90.43", 40011, udpPoller);
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException("failed to subscribe to NYSE market data", e);
        }
    }

    private void multicastSubscribe(
        final String networkInterface,
        final String addr,
        final int port,
        final UdpTransportPoller udpPoller)
        throws IOException
    {
        LOG.info("subscribe to {}:{} on {}", addr, port, networkInterface);
        DatagramChannel ch = DatagramChannel.open(StandardProtocolFamily.INET)
            .setOption(StandardSocketOptions.SO_REUSEADDR, true)
            .bind(new InetSocketAddress(port));  // bind to wildcard address
        ch.configureBlocking(false);

        NetworkInterface iface = NetworkInterface.getByName(networkInterface);
        InetAddress group = InetAddress.getByName(addr);
        ch.join(group, iface);
        udpPoller.registerForRead(ch);
    }
}
