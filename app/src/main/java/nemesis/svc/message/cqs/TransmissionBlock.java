package nemesis.svc.message.cqs;

import java.nio.ByteBuffer;
import java.time.Instant;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

//-----------------------------------------------------------------------------
// CQS_Pillar_Output_Specification 3.0
//-----------------------------------------------------------------------------
// class BlockHeader
//     byte  version;                    1 byte  (offset 0)
//     short blockSize;                  2 bytes (offset 1)
//     byte  dataFeedIndicator;          1 byte  (offset 3)
//     byte  retransmissionIndicator;    1 byte  (offset 4)
//     int   blockSequenceNumber;        4 bytes (offset 5)
//     byte  messagesInBlock;            1 byte  (offset 9)
//     long  sipBlockTimestamp;          8 bytes (offset 10)
//     short blockCheckSum;              2 bytes (offset 18)
//                                       total = 20 bytes

// class MessageHeader
//     short messageLength;              2 bytes (offset 0)
//     byte  messageCategory;            1 byte  (offset 2)
//     byte  messageType;                1 byte  (offset 3)
//     byte  participantID;              1 byte  (offset 4)
//     long  timestamp1;                 8 bytes (offset 5)
//     byte  messageID;                  1 byte  (offset 13)
//     int   transactionID;              4 bytes (offset 14)
//     long  participantReferenceNumber; 8 bytes (offset 18)
//                                       total = 26 bytes

public class TransmissionBlock extends SelfDescribingMarshallable
{
    public static int MAX_SIZE = 1024;
    public static int MSG_HEADER_SIZE = 26;

    private ByteBuffer buf;
    private int msgIndex;
    private int msgOffset;

    public void fromByteBuffer(ByteBuffer buf)
    {
        this.buf = buf;
        this.msgIndex = 0;
        this.msgOffset = 20;
    }

    public ByteBuffer buffer()
    {
        return this.buf;
    }

    public int messageOffset()
    {
        return this.msgOffset;
    }

    public short blockSize()
    {
        return buf.getShort(1);
    }

    public int blockSequenceNumber()
    {
        return buf.getInt(5);
    }

    public byte messagesInBlock()
    {
        return buf.get(9);
    }

    // sipBlockTimestamp is nanoseconds since epoch.
    public long sipBlockTimestamp()
    {
        long sec = buf.getInt(10);
        long nsec = buf.getInt(14);
        return sec * 1_000_000_000L + nsec;
    }

    public void setSipBlockTimestamp() throws Exception
    {
        Instant now = Instant.now();
        buf.putInt(10, (int) now.getEpochSecond());
        buf.putInt(14, now.getNano());
    }

    public short currMessageLength()
    {
        return buf.getShort(msgOffset);
    }

    public byte currMessageCategory()
    {
        return buf.get(msgOffset+2);
    }

    public byte currMessageType()
    {
        return buf.get(msgOffset+3);
    }

    public byte currParticipantID()
    {
        return buf.get(msgOffset+4);
    }

    // currTimestamp1 is nanosecond since epoch.
    public long currTimestamp1()
    {
        long sec = buf.getInt(msgOffset+5);
        long nsec = buf.getInt(msgOffset+9);
        return sec * 1_000_000_000L + nsec;
    }

    public byte currMessageID()
    {
        return buf.get(msgOffset+13);
    }

    public int currTransactionID()
    {
        return buf.getInt(msgOffset+14);
    }

    public long currParticipantReferenceNumber()
    {
        return buf.getLong(msgOffset+18);
    }

    public void nextMessage()
    {
        if (msgIndex+1 >= messagesInBlock())
            return;

        msgOffset += currMessageLength();
        msgIndex += 1;
    }

    public void displayBlockHeader()
    {
        System.out.printf("(%s) size: %d seq num: %d msgInBlk: %d timestamp: %d time: %s\n",
            buf.order().toString(),
            blockSize(),
            blockSequenceNumber(),
            messagesInBlock(),
            sipBlockTimestamp(),
            Instant.ofEpochSecond(
                sipBlockTimestamp() / 1_000_000_000L,
                sipBlockTimestamp() % 1_000_000_000L)
            .toString()
        );
    }

    public void displayMessageHeader()
    {
        System.out.printf("(%s) len: %d ctg: %c type: %c timestamp1: %s time: %s\n",
            buf.order().toString(),
            currMessageLength(),
            (char) currMessageCategory(),
            (char) currMessageType(),
            currTimestamp1(),
            Instant.ofEpochSecond(
                currTimestamp1() / 1_000_000_000L,
                currTimestamp1() % 1_000_000_000L)
            .toString()
        );
    }
}
