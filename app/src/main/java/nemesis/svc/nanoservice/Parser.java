package nemesis.svc.nanoservice;

import static nemesis.svc.nanoservice.Constant.BYTE_TO_INT_MASK;
import static nemesis.svc.nanoservice.Constant.INT_TO_LONG_MASK;
import static nemesis.svc.nanoservice.Util.retryPublicationResult;

import java.nio.ByteBuffer;

import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Publication;
import nemesis.svc.message.Quote;
import nemesis.svc.message.Trade;
import nemesis.svc.message.cqs.LongQuote;
import nemesis.svc.message.cqs.LongTrade;
import nemesis.svc.message.cqs.ShortTrade;
import nemesis.svc.message.cqs.TransmissionBlock;

public class Parser
{   
    private final Logger LOG;

    private final Publication pub;
    private final Quote quote;
    private final Trade trade;
    private final UnsafeBuffer quoteOutBuffer;
    private final UnsafeBuffer tradeOutBuffer;
    
    
    private final EpochNanoClock epochClock;
    private final NanoClock clock;
    private final long reportIntervalNs;
    private long nowNs;
    private long nextReportTimeNs;

    private long receivedBlocks;
    private long receivedMessages;
    private long lastSequenceNumber;
    private long missedMessages;
    private long unexpectedBlocks;
    private long lineIntegrityMessages;

    private long controlMessages;
    private long marketStatusMessages;
    private long quoteMessages;
    private long tradeMessages;
    private long unknownMessages;
    private long sentMessages;

    public Parser()
    {
        this(null);
    }


    public Parser(Publication pub)
    {
        this.LOG = LoggerFactory.getLogger(Parser.class);
        this.pub = pub;
        this.quote = new Quote(ByteBuffer.allocateDirect(Quote.MAX_SIZE));
        this.trade = new Trade(ByteBuffer.allocateDirect(Trade.MAX_SIZE));
        this.quoteOutBuffer = new UnsafeBuffer(quote.byteBuffer());
        this.tradeOutBuffer = new UnsafeBuffer(trade.byteBuffer());
        
        this.epochClock = new OffsetEpochNanoClock();
        this.clock  = new SystemNanoClock();
        this.reportIntervalNs = 10_000_000_000L;
        this.nowNs = clock.nanoTime();
        this.nextReportTimeNs = nowNs;

        this.receivedBlocks = 0;
        this.receivedMessages = 0;
        this.lastSequenceNumber = -1;
        this.missedMessages = 0;
        this.unexpectedBlocks = 0;
        this.lineIntegrityMessages = 0;

        this.controlMessages = 0;
        this.marketStatusMessages = 0;
        this.quoteMessages = 0;
        this.tradeMessages = 0;
        this.unknownMessages = 0;
    }

    public void onTransmissionBlock(TransmissionBlock block)
    {
        // block.displayHeader();
        long sequenceNumber = block.blockSequenceNumber() & INT_TO_LONG_MASK;
        int messagesInBlock = block.messagesInBlock() & BYTE_TO_INT_MASK;

        for (int i = 0; i < messagesInBlock; i++)
        {
            // block.displayMessageHeader();
            switch ((char) block.currMessageCategory())
            {
            case 'C':
                handleControlMessage(block);
                controlMessages += 1;
                break;

            case 'M':
                handleMarketStatusMessage();
                marketStatusMessages += 1;
                break;

            case 'Q':
                handleQuoteMessage(block);
                quoteMessages += 1;
                break;

            case 'T':
                handleTradeMessage(block);
                tradeMessages += 1;
                break;

            default:
                LOG.error("unknown message category: {}", (char) block.currMessageCategory());
                unknownMessages += 1;
                break;
            }
            block.nextMessage();
        }
        checkSequenceNumber(sequenceNumber, messagesInBlock);
    }

    private void handleControlMessage(TransmissionBlock block)
    {
        switch ((char) block.currMessageType())
        {
        // Reset block sequence number
        // Start of day, End of day
        case 'L':
        case 'A':
        case 'Z':
            this.lastSequenceNumber = -1;
            break;

        // Line integrity
        case 'T':
            this.lastSequenceNumber -= 1;
            this.lineIntegrityMessages += 1;
            break;
        }
    }

    private void handleMarketStatusMessage()
    {
        // LOG.info("handleMarketStatusMessage()");
        // TODO: 
    }

    private void handleQuoteMessage(TransmissionBlock block)
    {
        switch ((char) block.currMessageType())
        {
        case 'A', 'S', 'Q':
            // TODO: 
            // LOG.info("auction status or special long quote or short quote");
            break;

        case 'L':
            LongQuote.currBlockMessageToQuote(block, quote);
            quote.setReceivedAt(epochClock.nanoTime());
            if (pub != null && pub.isConnected())
            {
                sendMessage(quoteOutBuffer);
            }
            break;

        default:
            LOG.error("unknown quote message type: {}", (char) block.currMessageType());
            break;
        }
    }

    private void handleTradeMessage(TransmissionBlock block)
    {
        switch ((char) block.currMessageType())
        {
        case 'A', 'C', 'S', 'X':
            // TODO: 
            // LOG.info("auction status or trade correction or trading status or trade cancel/error");
            break;

        case 'L':
            LongTrade.currBlockMessageToTrade(block, trade);
            trade.setReceivedAt(epochClock.nanoTime());
            if (pub != null && pub.isConnected())
            {
                sendMessage(tradeOutBuffer);
            }
            break;

        case 'T':
            ShortTrade.currBlockMessageToTrade(block, trade);
            trade.setReceivedAt(epochClock.nanoTime());
            if (pub != null && pub.isConnected())
            {
                sendMessage(tradeOutBuffer);
            }
            break;

        default:
            LOG.error("unknown trade message type: {}", (char) block.currMessageType());
            break;
        }
    }

    private void checkSequenceNumber(long sequenceNumber, long messagesInBlock)
    {
        // Only start checking after receving first valid block.
        if (lastSequenceNumber > 0)
        {
            if (sequenceNumber < lastSequenceNumber)
            {
                LOG.error("received sequence number: {} less than previous sequence number: {}",
                    sequenceNumber, lastSequenceNumber);
                unexpectedBlocks += 1;
            }
            if (sequenceNumber > lastSequenceNumber + 1)
            {
                LOG.error("expected sequence number: {}, received: {}", lastSequenceNumber+1, sequenceNumber);
                missedMessages += sequenceNumber - (lastSequenceNumber + 1);
            }
        }
        lastSequenceNumber = sequenceNumber + messagesInBlock;
        receivedBlocks += 1;
        receivedMessages += messagesInBlock;
    }

    private void sendMessage(UnsafeBuffer outBuf)
    {
        long pos;
        while ((pos = pub.offer(outBuf)) <= 0)
        {
            if (!retryPublicationResult(pos))
                break;
        }
        if (pos > 0)
        {
            sentMessages += 1;
        }
    }

    public boolean onScheduleReport()
    {
        nowNs = clock.nanoTime();
        if (nextReportTimeNs <= nowNs)
        {
            nextReportTimeNs += reportIntervalNs;
            return true;
        }
        return false;
    }

    public void reportCounters()
    {
        LOG.info("-------------------------------------------------------------");
        LOG.info("        received blocks = {}", receivedBlocks);
        LOG.info("      received messages = {}", receivedMessages);
        LOG.info("       missing messages = {}", missedMessages);
        LOG.info("      unexpected blocks = {}", unexpectedBlocks);
        LOG.info("   last sequence number = {}", lastSequenceNumber);
        LOG.info("line integrity messages = {}", lineIntegrityMessages);
        LOG.info("       control messages = {}", controlMessages);
        LOG.info(" market status messages = {}", marketStatusMessages);
        LOG.info("         quote messages = {}", quoteMessages);
        LOG.info("         trade messages = {}", tradeMessages);
        LOG.info("       unknown messages = {}", unknownMessages);
        LOG.info("          sent messages = {}", unknownMessages);
    }
}