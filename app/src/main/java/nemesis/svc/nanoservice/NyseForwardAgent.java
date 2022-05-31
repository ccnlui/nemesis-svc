package nemesis.svc.nanoservice;

import java.nio.ByteBuffer;

import org.agrona.concurrent.Agent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Publication;
import nemesis.svc.message.cqs.TransmissionBlock;
import nemesis.svc.multicast.PacketHandler;
import nemesis.svc.multicast.UdpTransportPoller;

public class NyseForwardAgent implements Agent
{
    private static final Logger LOG = LoggerFactory.getLogger(NyseForwardAgent.class);

    private final Publication pub;
    private final UdpTransportPoller udpPoller;
    private final Parser parser;
    private final TransmissionBlock block;
    private final PacketHandler dataHandler;

    public NyseForwardAgent(final Publication pub, final UdpTransportPoller udpPoller)
    {
        this.pub = pub;
        this.udpPoller = udpPoller;
        this.parser = new Parser(this.pub);
        this.block = new TransmissionBlock();
        this.dataHandler = buf -> this.onBlock(buf);  // this is needed to avoid garbage
    }

    @Override
    public void onClose()
    {
        udpPoller.close();
    }

    @Override
    public int doWork() throws Exception
    {
        int bytesReceived = udpPoller.pollTransports(this.dataHandler);
        if (parser.onScheduleReport())
        {
            parser.reportCounters();
            // LOG.info("bytes received: {}", bytesReceived);
        }
        return bytesReceived;
    }

    @Override
    public String roleName()
    {
        return "nyse-forward";
    }

    private void onBlock(ByteBuffer buf)
    {
        block.fromByteBuffer(buf);
        parser.onTransmissionBlock(block);
    }
}
