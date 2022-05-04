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
import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.Callable;

import org.HdrHistogram.Histogram;

import nemesis.svc.message.cqs.TransmissionBlock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "listner",
    description = "subscribe and listen to marketdata multicast groups",
    usageHelpAutoWidth = true
)
public class Listener implements Callable<Void> {

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option(
        names = {"-i", "--interface"},
        defaultValue = "${NEMESIS_NETWORK_INTERFACE:-eth0}",
        description = "network interface"
    )
    String networkInterface;

    @Option(names = {"-b", "--bench"}, description = "measure inbound delay for 20 sec")
    boolean bench;

    // Constants.
    final long WARMUP_TIME_MSEC = 10_000;
    final long RUN_TIME_MSEC    = 20_000;

    // subscribe options
    // private final String msgType = "quote";
    // private final char tape = 'A';
    // private final String site = "NewYork";
    // private final char line = 'A';
    // private final String addr = "224.0.90.0";
    // private final int port = 40000;

    // private final int MAX_DATAGRAM_SIZE = 65535;  // unused

    @Override
    public Void call() throws Exception {

        final HashMap<DatagramChannel, String> subscribedGroups = new HashMap<>();

        final Bytes<ByteBuffer> bbb           = Bytes.elasticByteBuffer(TransmissionBlock.MAX_SIZE, TransmissionBlock.MAX_SIZE);
        final TransmissionBlock block         = new TransmissionBlock();
        final String            queuePathIce  = Config.queueBasePath + "/ice";

        long      startTime  = System.currentTimeMillis();
        Histogram lsnInDelay = new Histogram(60_000_000_000L, 3);;

        Selector sel = Selector.open();
        System.out.println("Subscribing to: 224.0.90.0:40000 on " + networkInterface);
        subscribe("224.0.90.0", 40000, sel, subscribedGroups);
        // subscribe("224.0.89.0", 40000, sel, subscribedGroups);

        try (
            SingleChronicleQueue outQueueIce = SingleChronicleQueueBuilder
                                                    .single(queuePathIce)
                                                    .rollCycle(Config.roleCycle)
                                                    .build();
        ) {
            final ExcerptAppender appender = outQueueIce.acquireAppender();

            // busy wait
            while (true) {
                sel.selectNow(key -> {
                    try {
                        long now = nowNano();

                        // get underlying ByteBuffer to work with nio
                        ByteBuffer buf = bbb.underlyingObject();
                        buf.clear();
                        DatagramChannel ch = (DatagramChannel) key.channel();
                        ch.receive(buf);
                        buf.flip();  // flip buffer for reading
                        bbb.readLimit(buf.remaining()); // update wrapper read cursor
        
                        // process message
                        block.fromByteBuffer(buf);
                        // block.parseHeader();
                        if (bench && (System.currentTimeMillis() - startTime > WARMUP_TIME_MSEC)) {
                            lsnInDelay.recordValue(now - block.sipBlockTimestamp());
                        }

                        // write raw bytes only
                        // appender.writeDocument(wire -> wire.writeBytes(b -> b.writeSome(buf)));

                        // write event and bytes
                        appender.writeDocument(wire -> {
                            wire.write("CQS").marshallable(m -> {
                                m.write("data").bytes(bbb);
                                m.write("rcvAt").int64(now);
                            });
                        });

                        // System.out.printf("%s: ", subscribedGroups.get(ch));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                if (bench && System.currentTimeMillis() - startTime > WARMUP_TIME_MSEC + RUN_TIME_MSEC) {
                    System.out.println("---------- lsnInDelay (us) ----------");
                    lsnInDelay.outputPercentileDistribution(System.out, 1000.0);  // output in us
                    break;
                }
            }
        }
        return null;
    }

    void subscribe(
        String addr,
        int port,
        Selector sel,
        HashMap<DatagramChannel, String> subscribedGroups
    ) throws IOException {
        DatagramChannel ch = DatagramChannel.open(StandardProtocolFamily.INET)
                                    .setOption(StandardSocketOptions.SO_REUSEADDR, true)
                                    .bind(new InetSocketAddress(port)); // bind to wildcard address
        ch.configureBlocking(false);
        NetworkInterface iface = NetworkInterface.getByName(networkInterface);
        InetAddress group = InetAddress.getByName(addr);
        
        // join and register with selector
        ch.join(group, iface);
        ch.register(sel, SelectionKey.OP_READ);
        subscribedGroups.put(ch, addr + ":" + port);
    }

    private long nowNano() {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }
}
