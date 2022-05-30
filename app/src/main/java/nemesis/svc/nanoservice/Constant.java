package nemesis.svc.nanoservice;

public final class Constant
{
    private Constant()
    {
    }

    public static final long INT_TO_LONG_MASK = 0x00_00_00_00_FF_FF_FF_FF;
    public static final int BYTE_TO_INT_MASK = 0x00_00_00_FF;

    // Quote appendage
    public static final byte noNBBOChange                    = (byte) 'A';  // No Best Bid change, No Best Offer change
	public static final byte quoteContainsAllNBBOInformation = (byte) 'G';  // Quote contains Best Bid, Quote contains Best Offer
	public static final byte noNBBO                          = (byte) 'O';  // No Best Bid, No Best Offer
	public static final byte shortFormNBBOAppendageAttached  = (byte) 'T';  // Short Form National BBO Appendage Attached
	public static final byte longFormNBBOAppendageAttached   = (byte) 'U';  // Best Bid Long Appendage, Best Offer Long Appendage
}
