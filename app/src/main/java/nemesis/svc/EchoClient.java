package nemesis.svc;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;
import java.util.concurrent.Callable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "echoclient",
    description = "udp multicast echo client",
    usageHelpAutoWidth = true
)
class EchoClient implements Callable<Void> {

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    private MulticastSocket socket;
    private byte[] buf = new byte[256];
    private ByteBuffer bb = ByteBuffer.allocate(256);

    @Override
    public Void call() throws Exception {
        // runClientIO();
        runClientNIO();
        return null;
    }

    void runClientIO() throws Exception {
        System.out.println("echo client...");
        socket = new MulticastSocket(4446);  // bind to wildcard address
        InetSocketAddress group = new InetSocketAddress("230.0.0.0", 0);  // port ignored.
        socket.joinGroup(group, socket.getNetworkInterface());

        while (true) {
            DatagramPacket pkt = new DatagramPacket(buf, buf.length);
            socket.receive(pkt); // blocking IO
            System.out.println(new String(pkt.getData()));
        }
    }

    void runClientNIO() throws Exception {

        System.out.println("echo client nio...");
        NetworkInterface iface;
        try (MulticastSocket s = new MulticastSocket()) {
            iface = s.getNetworkInterface();  // default iface
        }
        InetAddress group = InetAddress.getByName("230.0.0.0");
        DatagramChannel ch = DatagramChannel.open(StandardProtocolFamily.INET)
                                .setOption(StandardSocketOptions.SO_REUSEADDR, true)
                                .bind(new InetSocketAddress(4446));  // bind to wildcard address
        ch.configureBlocking(false);
        MembershipKey key = ch.join(group, iface);

        while (true) {
            if (ch.receive(bb) == null) {
                // System.out.println("empty");
                // Thread.sleep(100);
                continue;
            }
            bb.flip();
            while (bb.hasRemaining()) {
                System.out.print((char) bb.get());
            }
            System.out.println();
            bb.clear();
        }
    }
}
