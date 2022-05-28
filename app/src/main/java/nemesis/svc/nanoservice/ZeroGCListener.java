package nemesis.svc.nanoservice;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import org.agrona.BufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nemesis.svc.message.cqs.TransmissionBlock;
import nemesis.svc.multicast.PacketHandler;
import nemesis.svc.multicast.UdpTransportPoller;

public class ZeroGCListener
{
    private static final Logger LOG = LoggerFactory.getLogger(ZeroGCListener.class);

    private final UdpTransportPoller udpPoller;
    private final TransmissionBlock block;
    private final Parser parser;
    private final PacketHandler dataHandler;

    public ZeroGCListener()
    {
        final ByteBuffer inBuf = BufferUtil.allocateDirectAligned(Config.maxUdpMessageSize, 64);
        this.udpPoller = new UdpTransportPoller(inBuf, Throwable::printStackTrace);
        this.block = new TransmissionBlock();
        this.parser = new Parser();
        this.dataHandler = b -> this.onBlock(b);  // this is needed to avoid garbage
    }
    
    public void run() throws Exception
    {
        int bytesReceived = 0;
        multicastSubscribe(Config.networkInterface, Config.addr, Config.port, udpPoller);
        while (true)
        {
            bytesReceived += udpPoller.pollTransports(this.dataHandler);
            if (parser.onScheduleReport())
            {
                // parser.reportCounters();
                LOG.info("bytes received: {}", bytesReceived);
            }
        }
        // udpPoller.close();
    }

    public void onBlock(ByteBuffer buffer)
    {
        block.fromByteBuffer(buffer);
        block.displayHeader();
        // parser.onTransmissionBlock(block);
    }

    // multicastSubscribe has to be confined to the same thread as udp transport poller
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
