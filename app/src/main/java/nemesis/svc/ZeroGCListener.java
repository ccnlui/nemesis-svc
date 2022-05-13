package nemesis.svc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.PortUnreachableException;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.Callable;

import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.nio.TransportPoller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nemesis.svc.message.cqs.TransmissionBlock;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "zgc-listener", usageHelpAutoWidth = true,
    description = "subscribe to marketdata multicast and count sequence numbers")
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

    final class UdpTransportPoller extends TransportPoller
    {
        private static final DatagramChannel[] EMPTY_CHANNELS = new DatagramChannel[0];
        private DatagramChannel[] channels = EMPTY_CHANNELS;
        
        private final ByteBuffer byteBuffer;
        protected final ErrorHandler errorHandler;

        public UdpTransportPoller(final ByteBuffer byteBuffer, final ErrorHandler errorHandler)
        {
            this.byteBuffer = byteBuffer;
            this.errorHandler = errorHandler;
        }

        public void close()
        {
            for (final DatagramChannel ch : channels)
            {
                CloseHelper.close(errorHandler, ch);
            }
            super.close();
        }

        public int pollTransports(PacketHandler packetHandler)
        {
            int bytesReceived = 0;
            try
            {
                if (channels.length <= ITERATION_THRESHOLD_DEFAULT)
                {
                    for (final DatagramChannel ch : channels)
                    {
                        bytesReceived += poll(ch, packetHandler);
                    }
                }
                else
                {
                    selector.selectNow();

                    final SelectionKey[] keys = selectedKeySet.keys();
                    for (int i = 0, length = selectedKeySet.size(); i < length; i++)
                    {
                        bytesReceived += poll((DatagramChannel)keys[i].attachment(), packetHandler);
                    }

                    selectedKeySet.reset();
                }
            }
            catch (final IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            return bytesReceived;
        }

        public SelectionKey registerForRead(DatagramChannel ch)
        {
            SelectionKey key = null;
            try
            {
                key = ch.register(selector, SelectionKey.OP_READ, ch);
                channels = ArrayUtil.add(channels, ch);
            }
            catch (final ClosedChannelException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            return key;
        }

        public void cancelRead(DatagramChannel ch)
        {
            final DatagramChannel[] oldChannels = channels;
            int index = ArrayUtil.UNKNOWN_INDEX;

            for (int i = 0, length = channels.length; i < length; i++)
            {
                if (ch == oldChannels[i])
                {
                    index = i;
                    break;
                }
            }

            if (index != ArrayUtil.UNKNOWN_INDEX)
            {
                channels = (oldChannels.length == 1 ? EMPTY_CHANNELS : ArrayUtil.remove(oldChannels, index));
            }
        }

        private int poll(final DatagramChannel ch, PacketHandler packetHandler)
        {
            int bytesReceived = 0;
            byteBuffer.clear();

            InetSocketAddress srcAddress = null;
            try
            {
                if (ch.isOpen())
                {
                    srcAddress = (InetSocketAddress)ch.receive(byteBuffer);
                }
            }
            catch (final PortUnreachableException ignored)
            {
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            if (srcAddress != null)
            {
                bytesReceived = byteBuffer.position();
                byteBuffer.flip();
                packetHandler.onPacket(byteBuffer);
            }

            return bytesReceived;
        }
    }
}
