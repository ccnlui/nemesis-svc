package nemesis.svc.nanoservice;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;

import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.nio.TransportPoller;

public final class UdpTransportPoller extends TransportPoller
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

    public int poll(final DatagramChannel ch, PacketHandler packetHandler)
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
