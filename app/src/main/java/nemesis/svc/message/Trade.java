package nemesis.svc.message;
import java.nio.ByteBuffer;

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
        this.buf = ByteBuffer.allocateDirect(MAX_SIZE);
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
        buf.putLong(24, timestamp);
    }

    @Override
    public void setReceivedAt(long timestamp)
    {
        buf.putLong(46, timestamp);
    }
}