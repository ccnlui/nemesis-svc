package nemesis.svc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.graph.Network;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "subscribe",
    description = "subscribe to marketdata multicast groups",
    usageHelpAutoWidth = true
)
public class Subscribe implements Callable<Void> {

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    // subscribe options
    private final String msgType = "quote";
    private final char tape = 'A';
    private final String site = "NewYork";
    private final char line = 'A';
    private final String addr = "224.0.90.0";
    private final int port = 40000;

    private final int MAX_DATAGRAM_SIZE = 65535;


    @Override
    public Void call() throws Exception {
        System.out.println("subscribe!");
        subscribe();
        return null;
    }

    void subscribe() throws Exception {

        final ByteBuffer buf = ByteBuffer.allocateDirect(MAX_DATAGRAM_SIZE);
        final DatagramChannel ch = DatagramChannel.open(StandardProtocolFamily.INET)
                                    .setOption(StandardSocketOptions.SO_REUSEADDR, true)
                                    .bind(new InetSocketAddress(40000)); // bind to wildcard address
        ch.configureBlocking(false);
        NetworkInterface iface = NetworkInterface.getByName("en4");
        InetAddress group = InetAddress.getByName(addr);
        ch.join(group, iface);

        // register with selector
        Selector sel = Selector.open();
        ch.register(sel, SelectionKey.OP_READ);

        // busy wait
        while (true) {

            sel.selectNow(key -> {
                try {
                    buf.clear();
                    DatagramChannel currCh = (DatagramChannel) key.channel();
                    if (key.isReadable()) {
                        currCh.receive(buf);
                    }
                    buf.flip();
                    System.out.printf("%s (%s): ", currCh.getLocalAddress().toString(), currCh.toString());
                    while (buf.hasRemaining()) {
                        System.out.print((char) buf.get());
                    }
                    System.out.println();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

        }
    }
}
