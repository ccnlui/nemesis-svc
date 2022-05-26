package nemesis.svc.command;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.Callable;

import org.agrona.BufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nemesis.svc.message.cqs.TransmissionBlock;
import nemesis.svc.nanoservice.PacketHandler;
import nemesis.svc.nanoservice.Parser;
import nemesis.svc.nanoservice.UdpTransportPoller;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "zgc-listener",
    usageHelpAutoWidth = true,
    description = "subscribe to marketdata multicast without generate garbage")
public class ZeroGCListener implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option(names = {"-i", "--interface"}, defaultValue = "${NEMESIS_NETWORK_INTERFACE:-eth0}",
        description = "network interface")
    String networkInterface;

    @Option(names = "--addr", defaultValue = "224.0.90.0",
        description = "multicast group address to subscribe")
    String addr;

    @Option(names = "--port", defaultValue = "40000",
        description = "multicast group port to subscribe")
    int port;

    private static final Logger LOG = LoggerFactory.getLogger(ZeroGCListener.class);

    @Override
    public Void call() throws Exception
    {
        final TransmissionBlock block = new TransmissionBlock();
        final Parser parser = new Parser();
        final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(65504, 64);
        final UdpTransportPoller udpPoller = new UdpTransportPoller(byteBuffer, Throwable::printStackTrace);

        subscribe(addr, port, udpPoller);
        subscribe("224.0.90.32", 40000, udpPoller);

        PacketHandler handler = new PacketHandler()
        {
            @Override
            public void onPacket(ByteBuffer buffer)
            {
                block.fromByteBuffer(buffer);
                // block.parseHeader();
                // parser.onTransmissionBlock(block);
            }
        };

        int bytesReceived = 0;
        // busy wait
        while (true)
        {
            bytesReceived += udpPoller.pollTransports(handler);
            if (parser.onScheduleReport())
            {
                // parser.reportCounters();
                LOG.info("bytes received: {}", bytesReceived);
            }
        }

        // udpPoller.close();
    }

    void subscribe(String addr, int port, UdpTransportPoller udpPoller)
        throws IOException
    {
        LOG.info("subscribe to: {}:{}", addr, port);
        DatagramChannel ch = DatagramChannel.open(StandardProtocolFamily.INET)
            .setOption(StandardSocketOptions.SO_REUSEADDR, true)
            .bind(new InetSocketAddress(port));  // bind to wildcard address
        ch.configureBlocking(false);
        NetworkInterface iface = NetworkInterface.getByName(networkInterface);
        InetAddress group = InetAddress.getByName(addr);
        
        // join and register with selector
        ch.join(group, iface);

        udpPoller.registerForRead(ch);
    }
}
