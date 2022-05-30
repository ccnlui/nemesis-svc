package nemesis.svc.message.cqs;

import static nemesis.svc.nanoservice.Constant.BYTE_TO_INT_MASK;
import static nemesis.svc.nanoservice.Constant.INT_TO_LONG_MASK;

import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Parser
{   
    private final Logger LOG;
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


    public Parser()
    {
        this.LOG = LoggerFactory.getLogger(Parser.class);
        this.clock  = new SystemNanoClock();
        this.reportIntervalNs = 5_000_000_000L;
        this.nowNs = clock.nanoTime();
        this.nextReportTimeNs = nowNs;

        this.receivedBlocks = 0;
        this.receivedMessages = 0;
        this.lastSequenceNumber = -1;
        this.missedMessages = 0;
        this.unexpectedBlocks = 0;
        this.lineIntegrityMessages = 0;
    }

    public void onTransmissionBlock(TransmissionBlock block)
    {
        // block.parseHeader();
        long sequenceNumber = block.blockSequenceNumber() & INT_TO_LONG_MASK;
        int messagesInBlock = block.messagesInBlock() & BYTE_TO_INT_MASK;

        for (int i = 0; i < messagesInBlock; i++)
        {
            // block.parseMessageHeader();
            if ((char) block.currMessageCategory() == 'C')
            {
                switch ((char) block.currMessageType())
                {
                // Reset block sequence number
                // Start of day, End of day
                case 'L':
                case 'A':
                case 'Z':
                    lastSequenceNumber = -1;
                    break;

                // Line integrity
                case 'T':
                    lastSequenceNumber -= 1;
                    lineIntegrityMessages += 1;
                    break;
                }
            }
            block.nextMessage();
        }

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
        receivedBlocks += 1;
        receivedMessages += messagesInBlock;
        lastSequenceNumber = sequenceNumber + messagesInBlock;
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
    }
}
