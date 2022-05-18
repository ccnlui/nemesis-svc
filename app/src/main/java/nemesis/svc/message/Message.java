package nemesis.svc.message;

// class Message
// {
//     byte   type;  //   1 byte  (offset 0)
//     byte[] data;  // variable  (offset 1)
// }
public interface Message
{
    public static int QUOTE = 0;
    public static int TRADE = 1;
    public static int MAX_SIZE = 128;

    public abstract void fromByteBuffer(java.nio.ByteBuffer buf);

    public abstract java.nio.ByteBuffer byteBuffer();

    public abstract int type();

    // setTimestamp is used only for testing purposes.
    public abstract void setTimestamp(long ts);

    // setReceivedAt is used only for testing purposes.
    public abstract void setReceivedAt(long ts);
}
