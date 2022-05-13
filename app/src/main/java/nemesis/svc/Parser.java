package nemesis.svc;

import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nemesis.svc.message.cqs.TransmissionBlock;

public class Parser
{
    private final Logger LOG = LoggerFactory.getLogger(Parser.class);

    private final NanoClock clock = new SystemNanoClock();
    private final long reportItvNs = 5_000_000_000L;
    private long nowNs = clock.nanoTime();
    private long nextReportTimeNs = nowNs;

    private long receivedBlocks = 0;
    private long receivedMessages = 0;
    private long lastSequenceNumber = -1;
    private long missedMessages = 0;
    private long unexpectedBlocks = 0;
    private long lineIntegrityMessages = 0;

    private long INT_TO_LONG_MASK = 0x00_00_00_00_FF_FF_FF_FF;
    private int BYTE_TO_INT_MASK = 0x00_00_00_FF;

    void onTransmissionBlock(TransmissionBlock block)
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
                case 'L', 'A', 'Z':
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

    boolean onScheduleReport()
    {
        nowNs = clock.nanoTime();
        if (nextReportTimeNs <= nowNs)
        {
            nextReportTimeNs += reportItvNs;
            return true;
        }
        return false;
    }

    void reportCounters()
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
