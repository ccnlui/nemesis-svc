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
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import nemesis.svc.message.cqs.TransmissionBlock;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
(
    name = "publisher",
    description = "listen and publish marketdata multicast data",
    usageHelpAutoWidth = true
)
public class Publisher implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option
    (
        names = {"-i", "--interface"},
        defaultValue = "${NEMESIS_NETWORK_INTERFACE:-eth0}",
        description = "network interface"
    )
    String networkInterface;

    @Option(names = {"-b", "--bench"}, description = "measure inbound delay for 20 sec")
    boolean bench;
    
    private static final Logger LOG = LoggerFactory.getLogger(Publisher.class);

    final long WARMUP_TIME_MSEC = 10_000;
    final long RUN_TIME_MSEC    = 20_000;

    @Override
    public Void call() throws Exception
    {
        final HashMap<DatagramChannel, String> subscribedGroups = new HashMap<>();
        final UnsafeBuffer usb = new UnsafeBuffer(ByteBuffer.allocateDirect(TransmissionBlock.MAX_SIZE));
        final TransmissionBlock block = new TransmissionBlock();
        long startTime  = System.currentTimeMillis();
        Histogram pbsInDelay = new Histogram(60_000_000_000L, 3);

        Selector sel = Selector.open();
        LOG.info("Subscribing to: 224.0.90.0:40000 on {}", networkInterface);
        subscribe("224.0.90.0", 40000, sel, subscribedGroups);
        // subscribe("224.0.89.0", 40000, sel, subscribedGroups);

        DatagramChannel ch = subscribedGroups.keySet().iterator().next();
        ByteBuffer buf = usb.byteBuffer();

        final String channel = "aeron:udp?endpoint=127.0.0.1:2000|mtu=1408";
        final int stream = 10;

        // construct media driver, clean up media driver folder on start/stop
        final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new BusySpinIdleStrategy())
            .dirDeleteOnShutdown(true);
        final MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaDriverCtx);

        // construct aeron, point at the media driver's folder
        final Aeron.Context aeronCtx = new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName());
        final Aeron aeron = Aeron.connect(aeronCtx);

        LOG.info("Dir: {}", mediaDriver.aeronDirectoryName());

        final Publication pub = aeron.addPublication(channel, stream);

        // busy wait
        while (true)
        {
            if (ch.receive(buf) != null)
            {
                long now = nowNano();
                buf.flip();  // flip buffer for reading
                usb.wrap(buf, buf.position(), buf.remaining()); // update wrapper

                // process message
                block.fromByteBuffer(buf);
                if (bench && (System.currentTimeMillis() - startTime > WARMUP_TIME_MSEC))
                {
                    long d = now - block.sipBlockTimestamp();
                    if (d < 0) {
                        d = 42;
                    }
                    pbsInDelay.recordValue(d);
                }

                block.setSipBlockTimestamp();
                // block.parseHeader();
                if (pub.isConnected())
                {
                    if (pub.offer(usb) < 0)
                    {
                        LOG.debug("failed to offer message");
                    }
                }

                buf.clear();
            }

            if (bench && System.currentTimeMillis() - startTime > WARMUP_TIME_MSEC + RUN_TIME_MSEC)
            {
                LOG.info("---------- pbsInDelay (us) ----------");
                pbsInDelay.outputPercentileDistribution(System.out, 1000.0);  // output in us
                break;
            }
        }

        // clean up
        pub.close();
        aeron.close();
        mediaDriver.close();
        
        return null;
    }

    void subscribe (
        String addr,
        int port,
        Selector sel,
        HashMap<DatagramChannel, String> subscribedGroups
    ) throws IOException
    {
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
