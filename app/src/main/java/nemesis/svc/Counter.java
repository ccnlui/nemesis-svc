package nemesis.svc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nemesis.svc.message.cqs.TransmissionBlock;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "counter",
    description = "subscribe to marketdata multicast and count sequence numbers",
    usageHelpAutoWidth = true
)
public class Counter implements Callable<Void>
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

    // subscribe options
    // private final String msgType = "quote";
    // private final char tape = 'A';
    // private final String site = "NewYork";
    // private final char line = 'A';
    // private final String addr = "224.0.90.0";
    // private final int port = 40000;

    // private final int MAX_DATAGRAM_SIZE = 65535;  // unused

    private static final Logger LOG = LoggerFactory.getLogger(Counter.class);

    @Override
    public Void call() throws Exception
    {
        final HashMap<DatagramChannel, String> subscribedGroups = new HashMap<>();
        final TransmissionBlock block = new TransmissionBlock();
        final ByteBuffer buf = ByteBuffer.allocateDirect(TransmissionBlock.MAX_SIZE);
        final Selector sel = Selector.open();  // unused
        final Parser parser = new Parser();

        subscribe(addr, port, sel, subscribedGroups);

        // Only works because we have 1 channel.
        DatagramChannel ch = subscribedGroups.keySet().iterator().next();
        // busy wait
        while (true)
        {
            if (ch.receive(buf) != null)
            {
                buf.flip();  // flip buffer for reading
                block.fromByteBuffer(buf);
                parser.onTransmissionBlock(block);
                buf.clear();
            }
            if (parser.onScheduleReport())
            {
                parser.reportCounters();
            }
        }
    }

    void subscribe(
        String addr,
        int port,
        Selector sel,
        HashMap<DatagramChannel, String> subscribedGroups
    ) throws IOException
    {
        DatagramChannel ch = DatagramChannel.open(StandardProtocolFamily.INET)
            .setOption(StandardSocketOptions.SO_REUSEADDR, true)
            .setOption(StandardSocketOptions.SO_RCVBUF, Integer.MAX_VALUE)
            .bind(new InetSocketAddress(port));  // bind to wildcard address
        ch.configureBlocking(false);
        NetworkInterface iface = NetworkInterface.getByName(networkInterface);
        InetAddress group = InetAddress.getByName(addr);
        
        // join and register with selector
        ch.join(group, iface);
        ch.register(sel, SelectionKey.OP_READ);
        subscribedGroups.put(ch, addr + ":" + port);
        LOG.info("subscribed to: {}:{} rcvbuf: {}", addr, port, ch.getOption(StandardSocketOptions.SO_RCVBUF));
    }
}
