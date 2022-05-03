package nemesis.svc.message.cqs;

import java.nio.ByteBuffer;
import java.time.Instant;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

// CQS_Pillar_Output_Specification 3.0
// class Header {
//     byte  version;                 // 1 byte  (offset 0)
//     short blockSize;               // 2 bytes (offset 1)
//     byte  dataFeedIndicator;       // 1 byte  (offset 3)
//     byte  retransmissionIndicator; // 1 byte  (offset 4)
//     int   blockSequenceNumber;     // 4 bytes (offset 5)
//     byte  messagesInBlock;         // 1 byte  (offset 9)
//     long  sipBlockTimestamp;       // 8 bytes (offset 10)
//     short blockCheckSum;           // 2 bytes (offset 18)
// }                                  // total = 20 bytes
public class TransmissionBlock extends SelfDescribingMarshallable {

    public static int MAX_SIZE = 1024;
    ByteBuffer buf;

    public void fromByteBuffer(ByteBuffer buf) {
        this.buf = buf;
    }

    public short blockSize() {
        return buf.getShort(1);
    }

    public int blockSequenceNumber() {
        return buf.getInt(5);
    }

    public byte messagesInBlock() {
        return buf.get(9);
    }

    // sipBlockTimestamp is nanoseconds since epoch.
    public long sipBlockTimestamp() {
        long sec = buf.getInt(10);
        long nsec = buf.getInt(14);
        return sec * 1_000_000_000L + nsec;
    }

    public void parseHeader() {
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
}
