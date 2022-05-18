package nemesis.svc.message;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

import org.agrona.concurrent.SystemEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

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

    // setFakeValues initialize fake values for testing purposes.
    // it generates garbage
    public void setFakeValues(ByteBuffer buf)
    {
        fromByteBuffer(buf);
        setType();
        setSymbol("FAKEPACA", Charset.forName("ISO-8859-1"));
        setAskExchange((byte) 'A');
        setBidExchange((byte) 'B');
        setConditions(new byte[]{(byte) 'R'});
        setAskPrice(678.90);
        setBidPrice(123.45);
        setAskSize(11);
        setBidSize(22);
        setNbbo((byte) 1);
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
        buf.put(0, (byte) Message.QUOTE);
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

    public byte askExchange()
    {
        return buf.get(12);
    }

    public void setAskExchange(byte ae)
    {
        buf.put(12, ae);
    }

    public byte bidExchange()
    {
        return buf.get(13);
    }

    public void setBidExchange(byte be)
    {
        buf.put(13, be);
    }

    public int conditions(ByteBuffer bb)
    {
        int i;
        for (i = 0; i < 2; i++)
        {
            bb.put(buf.get(14+i));
        }
        return i;
    }

    public void setConditions(byte[] conditions)
    {
        buf.put(14, conditions);
    }

    public long timestamp()
    {
        return buf.getLong(16);
    }

    public void setTimestamp(long timestamp)
    {
        buf.putLong(16, timestamp);
    }

    public double askPrice()
    {
        return buf.getDouble(24);
    }

    public void setAskPrice(double ap)
    {
        buf.putDouble(24, ap);
    }

    public double bidPrice()
    {
        return buf.getDouble(32);
    }

    public void setBidPrice(double bp)
    {
        buf.putDouble(32, bp);
    }

    public int askSize()
    {
        return buf.getInt(40);
    }

    public void setAskSize(int as)
    {
        buf.putInt(40, as);
    }

    public int bidSize()
    {
        return buf.getInt(44);
    }

    public void setBidSize(int bs)
    {
        buf.putInt(44, bs);
    }

    public byte nbbo()
    {
        return buf.get(48);
    }

    public void setNbbo(byte nbbo)
    {
        buf.put(48, nbbo);
    }

    public byte tape()
    {
        return buf.get(49);
    }

    public void setTape(byte tape)
    {
        buf.put(49, tape);
    }
    
    public long receivedAt()
    {
        return buf.getLong(50);
    }

    public void setReceivedAt(long timestamp)
    {
        buf.putLong(50, timestamp);
    }

    public int toMessageJson(UnsafeBuffer out)
    {
        int pos = 0;

        // Type.
        pos += out.putStringWithoutLengthAscii(pos, "[{\"T\":\"q\"");

        // // Symbol.
        // pos += out.putStringWithoutLengthAscii(pos, ",\"S\":\"");
        // for (int i = 0; i < 11; i++)
        // {
        //     byte b = buf.get(1+i);
        //     if (MessageUtil.isAsciiPrintable(b))
        //     {
        //         out.putByte(pos++, b);
        //     }
        // }
        // out.putByte(pos++, (byte)'\"');

        // // AskExchange.
        // pos += out.putStringWithoutLengthAscii(pos, ",\"ax\":\"");
        // // TODO
        // // out.putByte(pos++, askExchange());
        // out.putByte(pos++, (byte)'\"');

        // // BidExchange.
        // pos += out.putStringWithoutLengthAscii(pos, ",\"bx\":\"");
        // // TODO
        // // out.putByte(pos++, bidExchange());
        // out.putByte(pos++, (byte)'\"');

        // // Conditions.
        // pos += out.putStringWithoutLengthAscii(pos, ",\"c\":[");
        // for (int i = 0; i < 2; i++)
        // {
        //     byte b = buf.get(14+i);
        //     if (MessageUtil.isAsciiPrintable(b))
        //     {
        //         out.putByte(pos++, b);
        //     }
        // }
        // out.putByte(pos++, (byte)']');

        // Timestamp nanos.
        pos += out.putStringWithoutLengthAscii(pos, ",\"tn\":");
        pos += out.putLongAscii(pos, timestamp());

        // // AskPrice.
        // pos += out.putStringWithoutLengthAscii(pos, ",\"ap\":");
        // // TODO
        // pos += out.putStringWithoutLengthAscii(pos, "0.0");

        // // BidPrice.
        // pos += out.putStringWithoutLengthAscii(pos, ",\"bp\":");
        // // TODO
        // pos += out.putStringWithoutLengthAscii(pos, "0.0");

        // // AskSize.
        // pos += out.putStringWithoutLengthAscii(pos, ",\"as\":");
        // pos += out.putIntAscii(pos, askSize());

        // // BidSize.
        // pos += out.putStringWithoutLengthAscii(pos, ",\"bs\":");
        // pos += out.putIntAscii(pos, bidSize());

        // // Tape.
        // pos += out.putStringWithoutLengthAscii(pos, ",\"z\":\"");
        // // TODO
        // // out.putByte(pos++, tape());
        // out.putByte(pos++, (byte)'\"');

        pos += out.putStringWithoutLengthAscii(pos, "}]");
        return pos;
    }

    public int toMessageMsgpack(UnsafeBuffer out)
    {
        int pos = 0;

        // array header
        out.putByte(pos++, (byte) 0x91);

        // map header (12 fields)
        out.putByte(pos++, (byte) 0x8c);

        // Type.
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 'T');
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 'q');

        // Symbol.
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 'S');
        int sl = symbolLength();
        out.putByte(pos++, (byte) (0xa0 + sl));
        out.putBytes(pos, this.buf, 1, sl);
        pos += sl;

        // BidExchange.
        out.putByte(pos++, (byte) 0xa2);
        pos += out.putStringWithoutLengthAscii(pos, "bx");
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) bidExchange());

        // BidPrice.
        out.putByte(pos++, (byte) 0xa2);
        pos += out.putStringWithoutLengthAscii(pos, "bp");
        out.putByte(pos++, (byte) 0xcb);
        out.putDouble(pos, bidPrice(), ByteOrder.BIG_ENDIAN);
        pos += Double.BYTES;

        // BidSize.
        out.putByte(pos++, (byte) 0xa2);
        pos += out.putStringWithoutLengthAscii(pos, "bs");
        out.putByte(pos++, (byte) 0xce);
        out.putInt(pos, bidSize(), ByteOrder.BIG_ENDIAN);
        pos += Integer.BYTES;

        // AskExchange.
        out.putByte(pos++, (byte) 0xa2);
        pos += out.putStringWithoutLengthAscii(pos, "ax");
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) askExchange());

        // AskPrice.
        out.putByte(pos++, (byte) 0xa2);
        pos += out.putStringWithoutLengthAscii(pos, "ap");
        out.putByte(pos++, (byte) 0xcb);
        out.putDouble(pos, askPrice(), ByteOrder.BIG_ENDIAN);
        pos += Double.BYTES;

        // AskSize.
        out.putByte(pos++, (byte) 0xa2);
        pos += out.putStringWithoutLengthAscii(pos, "as");
        out.putByte(pos++, (byte) 0xce);
        out.putInt(pos, askSize(), ByteOrder.BIG_ENDIAN);
        pos += Integer.BYTES;

        // Conditions.
        out.putByte(pos++, (byte) 0xa1);
        out.putByte(pos++, (byte) 'c');
        int cl = conditionsLength();
        out.putByte(pos++, (byte) (0x90 + cl));
        for (int i = 0; i < cl; i++)
        {
            out.putByte(pos++, (byte) 0xa1);
            out.putByte(pos++, this.buf.get(14+i));
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
        for (len = 0; len < 2; len++)
        {
            byte b = buf.get(14+len);
            if (!MessageUtil.isAsciiPrintable(b))
                break;
        }
        return len;
    }
}