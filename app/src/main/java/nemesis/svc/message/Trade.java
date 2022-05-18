package nemesis.svc.message;
import java.nio.ByteBuffer;

import org.agrona.concurrent.UnsafeBuffer;

// class Trade
// {
//     byte   type;                    //  1 byte  (offset 0)
//     byte[] symbol;                  // 11 bytes (offset 1)
//     int    volume;                  //  4 bytes (offset 12)
//     long   id;                      //  8 bytes (offset 16)
//     long   timestamp;               //  8 bytes (offset 24)
//     double price;                   //  8 bytes (offset 32)
//     byte[] conditions;              //  4 bytes (offset 40)
//     byte   exchange;                //  1 byte  (offset 44)
//     byte   tape;                    //  1 byte  (offset 45)
//     long   receivedAt;              //  8 bytes (offset 46)
// }                                   // total = 54 bytes
public class Trade implements Message
{
    public static int MAX_SIZE = 54;
    private ByteBuffer buf;

    public Trade()
    {
    }

    public Trade(ByteBuffer buf)
    {
        this.buf = buf;
        this.buf.put(0, (byte) Message.TRADE);
    }

    @Override
    public void fromByteBuffer(ByteBuffer buf)
    {
        this.buf = buf;
    }

    @Override
    public ByteBuffer byteBuffer()
    {
        return this.buf;
    }

    @Override
    public int type()
    {
        return buf.get(0);
    }

    public void setTimestamp(long timestamp)
    {
        buf.putLong(24, timestamp);
    }

    public void setReceivedAt(long timestamp)
    {
        buf.putLong(46, timestamp);
    }

    public long timestamp()
    {
        return buf.getLong(24);
    }

    public long receivedAt()
    {
        return buf.getLong(46);
    }

    public int toMessageJson(UnsafeBuffer out)
    {
        int pos = 0;
        pos += out.putStringWithoutLengthAscii(pos, "[{\"T\":\"t\",\"tn\":");
        pos += out.putLongAscii(pos, timestamp());
        pos += out.putStringWithoutLengthAscii(pos, "}]");
        return pos;
    }
}