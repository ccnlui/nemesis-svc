package nemesis.svc.message;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

import org.agrona.concurrent.SystemEpochNanoClock;
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

    public void setFakeValues(ByteBuffer buf)
    {
        fromByteBuffer(buf);
        setType();
        setSymbol("FAKEPACA", Charset.forName("ISO-8859-1"));
        setVolume(100);
        setID(42);
        setPrice(123.45);
        setConditions(new byte[]{(byte) 'I', '@'});
        setExchange((byte) 'N');
        setTape((byte) 'A');

        SystemEpochNanoClock clock = new SystemEpochNanoClock();
        long epochNs = clock.nanoTime();
        setTimestamp(epochNs - 1_000_000L);
        setReceivedAt(epochNs);
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

    public void setType()
    {
        buf.put(0, (byte) Message.TRADE);
    }

    public int symbol(StringBuffer sb)
    {
        int i;
        for (i = 0; i < 11; i++)
        {
            byte b = buf.get(1+i);
            if (MessageUtil.isAsciiPrintable(b))
                sb.append((char) b);
            else
                break;
        }
        return i;
    }

    public void setSymbol(String symbol, Charset charset)
    {
        buf.put(1, symbol.getBytes(charset));
    }

    public int volume()
    {
        return buf.getInt(12);
    }

    public void setVolume(int volume)
    {
        buf.putInt(12, volume);
    }

    public long ID()
    {
        return buf.getLong(16);
    }

    public void setID(long id)
    {
        buf.putLong(16, id);
    }

    public long timestamp()
    {
        return buf.getLong(24);
    }

    public void setTimestamp(long timestamp)
    {
        buf.putLong(24, timestamp);
    }

    public double price()
    {
        return buf.getDouble(32);
    }

    public void setPrice(double price)
    {
        buf.putDouble(32, price);
    }

    public int conditions(ByteBuffer bb)
    {
        int i;
        for (i = 0; i < 4; i++)
        {
            bb.put(buf.get(40+i));
        }
        return i;
    }

    public void setConditions(byte[] conditions)
    {
        buf.put(40, conditions);
    }

    public byte exchange()
    {
        return buf.get(44);
    }

    public void setExchange(byte exchange)
    {
        buf.put(44, exchange);
    }

    public byte tape()
    {
        return buf.get(45);
    }

    public void setTape(byte tape)
    {
        buf.put(45, tape);
    }

    public void setReceivedAt(long timestamp)
    {
        buf.putLong(46, timestamp);
    }

    public long receivedAt()
    {
        return buf.getLong(46);
    }

    @Override
    public int toMessageData(Format format, UnsafeBuffer out)
    {
        int bytes = switch (format)
        {
            case MSGPACK -> toMessageMsgpack(out);
            case JSON -> toMessageJson(out);
        };
        return bytes;
    }

    public int toMessageJson(UnsafeBuffer out)
    {
        int pos = 0;
        pos += out.putStringWithoutLengthAscii(pos, "[{\"T\":\"t\",\"tn\":");
        pos += out.putLongAscii(pos, timestamp());
        pos += out.putStringWithoutLengthAscii(pos, "}]");
        return pos;
    }

    public int toMessageMsgpack(UnsafeBuffer out)
    {
        int pos = 0;

        // array header
        out.putByte(pos++, (byte) 0x91);

        // map header (10 fields)
        out.putByte(pos++, (byte) 0x8a);

        // Type.
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 'T');
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 't');

        // Symbol.
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 'S');
        int sl = symbolLength();
        out.putByte(pos++, (byte) (0xa0 + sl));
        out.putBytes(pos, this.buf, 1, sl);
        pos += sl;

        // ID.
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 'i');
        out.putByte(pos++, (byte) 0xd3);
        out.putLong(pos, ID(), ByteOrder.BIG_ENDIAN);
        pos += Long.BYTES;

        // Exchange.
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 'x');
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, exchange());

        // Price.
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 'p');
        out.putByte(pos++, (byte) 0xcb);
        out.putDouble(pos, price(), ByteOrder.BIG_ENDIAN);
        pos += Double.BYTES;

        // Size.
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 's');
        out.putByte(pos++, (byte) 0xce);
        out.putInt(pos, volume(), ByteOrder.BIG_ENDIAN);
        pos += Integer.BYTES;

        // Conditions.
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 'c');
        int cl = conditionsLength();
        out.putByte(pos++, (byte) (0x90 + cl));
        for (int i = 0; i < cl; i++)
        {
            out.putByte(pos++, (byte) 0xa1);
            out.putByte(pos++, this.buf.get(40+i));
        }

        // Tape.
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 'z');
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, tape());

        // Timestamp.
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 't');
        out.putByte(pos++, (byte) 0xd7);
        out.putByte(pos++, (byte) 0xff);
        long ts = ((timestamp() % 1_000_000_000L) << 34) | timestamp() / 1_000_000_000L;
        out.putLong(pos, ts, ByteOrder.BIG_ENDIAN);
        pos += Long.BYTES;

        // ReceivedAt.
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 'r');
        out.putByte(pos++, (byte) 0xd7);
        out.putByte(pos++, (byte) 0xff);
        long rcv = ((receivedAt() % 1_000_000_000L) << 34) | receivedAt() / 1_000_000_000L;
        out.putLong(pos, rcv, ByteOrder.BIG_ENDIAN);
        pos += Long.BYTES;

        return pos;
    }

    private int symbolLength()
    {
        int len;
        for (len = 0; len < 11; len++)
        {
            byte b = buf.get(1+len);
            if (!MessageUtil.isAsciiPrintable(b))
                break;
        }
        return len;
    }

    private int conditionsLength()
    {
        int len;
        for (len = 0; len < 4; len++)
        {
            byte b = buf.get(40+len);
            if (!MessageUtil.isAsciiPrintable(b))
                break;
        }
        return len;
    }
}