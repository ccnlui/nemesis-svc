package nemesis.svc.message;

final class MessageUtil
{
    static boolean isAsciiPrintable(char ch)
    {
        return ch >= 32 && ch < 127;
    }

    static boolean isAsciiPrintable(byte b)
    {
        return b >= 32 && b < 127;
    }
}
