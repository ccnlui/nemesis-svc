package nemesis.svc.message;

import java.nio.ByteBuffer;

// class Quote
// {
//     byte   type;                    //  1 byte  (offset 0)
//     byte[] symbol;                  // 11 bytes (offset 1)
//     byte   askExchange;             //  1 byte  (offset 12)
//     byte   bidExchange;             //  1 byte  (offset 13)
//     byte[] conditions;              //  2 bytes (offset 14)
//     long   timestamp;               //  8 bytes (offset 16)
//     double askPrice;                //  8 bytes (offset 24)
//     double bidPrice;                //  8 bytes (offset 32)
//     int    askSize;                 //  4 bytes (offset 40)
//     int    bidSize;                 //  4 bytes (offset 44)
//     byte   nbbo;                    //  1 byte  (offset 48)
//     byte   tape;                    //  1 byte  (offset 49)
//     long   receivedAt;              //  8 bytes (offset 50)
// }                                   // total = 58 bytes
public class Quote implements Message
{
    public static int MAX_SIZE = 58;
    private ByteBuffer buf;

    public Quote()
    {
    }

    public Quote(ByteBuffer buf)
    {
        this.buf = buf;
        this.buf.put(0, (byte) Message.QUOTE);
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

    @Override
    public void setTimestamp(long timestamp)
    {
        buf.putLong(16, timestamp);
    }

    @Override
    public void setReceivedAt(long timestamp)
    {
        buf.putLong(50, timestamp);
    }

    public long receivedAt()
    {
        return buf.getLong(50);
    }
}