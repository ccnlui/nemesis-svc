package nemesis.svc.nanoservice;

import static nemesis.svc.nanoservice.Constant.BYTE_TO_INT_MASK;
import static nemesis.svc.nanoservice.Constant.INT_TO_LONG_MASK;

import java.nio.ByteBuffer;

import org.agrona.concurrent.Agent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Publication;
import nemesis.svc.message.Quote;
import nemesis.svc.message.Trade;
import nemesis.svc.message.cqs.LongQuote;
import nemesis.svc.message.cqs.Parser;
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

    private final Quote quote;
    private final Trade trade;

    private final PacketHandler dataHandler;

    public NyseForwardAgent(final Publication pub, final UdpTransportPoller udpPoller)
    {
        this.pub = pub;
        this.udpPoller = udpPoller;
        this.parser = new Parser();

        this.block = new TransmissionBlock();

        this.quote = new Quote(ByteBuffer.allocateDirect(Quote.MAX_SIZE));
        this.trade = new Trade(ByteBuffer.allocateDirect(Trade.MAX_SIZE));

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
            // parser.reportCounters();
            LOG.info("bytes received: {}", bytesReceived);
        }
        return bytesReceived;
    }

    @Override
    public String roleName()
    {
        return "nyse-forward";
    }

    private void onBlock(ByteBuffer buffer)
    {
        block.fromByteBuffer(buffer);

        long sequenceNumber = block.blockSequenceNumber() & INT_TO_LONG_MASK;
        int messagesInBlock = block.messagesInBlock() & BYTE_TO_INT_MASK;

        for (int i = 0; i < messagesInBlock; i++)
        {
            switch ((char) block.currMessageCategory())
            {
            case 'C':
                LOG.info("category C");
                handleControlMessage();
                break;

            case 'M':
                LOG.info("category M");
                handleMarketStatusMessage();
                break;

            case 'Q':
                LOG.info("category Q");
                handleQuoteMessage(block);
                break;

            case 'T':
                LOG.info("category T");
                handleTradeMessage(block);
                break;
            }
        }
    }

    private void handleControlMessage()
    {
        LOG.info("handleControlMessage()");
        // TODO: 
    }

    private void handleMarketStatusMessage()
    {
        LOG.info("handleMarketStatusMessage()");
        // TODO: 
    }

    private void handleQuoteMessage(TransmissionBlock block)
    {
        LOG.info("handleQuoteMessage()");
        switch ((char )block.currMessageType())
        {
        case 'A', 'S', 'Q':
            LOG.info("auction status or special long quote or short quote");
            break;

        case 'L':
            LongQuote.currBlockMessageToQuote(block, quote);
            break;

        default:
            LOG.error("unknown quote message type");
            break;
        }
    }

    private void handleTradeMessage(TransmissionBlock block)
    {
        LOG.info("handleTradeMessage()");
        switch ((char )block.currMessageType())
        {
        case 'A', 'C', 'S', 'X':
            LOG.info("auction status or trade correction or trading status or trade cancel/error");
            break;

        case 'L':
            // LongTrade.currBlockMessageToTrade(block, trade);
            break;

        case 'T':
            // ShortTrade.currBlockMessageToTrade(block, trade);
            break;

        default:
            LOG.error("unknown trade message type");
            break;
        }
    }
}
