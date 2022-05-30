package nemesis.svc.message.cqs;

import java.nio.ByteBuffer;

import nemesis.svc.message.Quote;

//-----------------------------------------------------------------------------
// CQS_Pillar_Output_Specification 6.4.2
//-----------------------------------------------------------------------------
// class LongQuote
//     byte[] securitySymbol;                    1 bytes (offset 0)
//     byte   instrumentType;                    1 byte  (offset 11)
//     byte   quoteCondition;                    1 byte  (offset 12)
//     byte   securityStatusIndicator;           1 byte  (offset 13)
//     long   bidPrice;                          8 bytes (offset 14)
//     int    bidSize;                           4 bytes (offset 22)
//     long   offerPrice;                        8 bytes (offset 26)
//     int    offerSize;                         4 bytes (offset 34)
//     byte   retailInterestIndicator;           1 byte  (offset 38)
//     byte   settlementCondition;               1 byte  (offset 39)
//     byte   marketCondition;                   1 byte  (offset 40)
//     byte[] finraMarketMakerID;                4 bytes (offset 41)
//     byte   finraBBOIndicator;                 1 byte  (offset 45)
//     long   timestamp2;                        8 bytes (offset 46)
//     byte   shortSaleRestrictionIndicator;     1 byte  (offset 54)
//     byte   primaryListingMarketParticipantID; 1 byte  (offset 55)
//     byte   financialStatusIndicator;          1 byte  (offset 56)
//     byte   sipGeneratedMessageIdentifier;     1 byte  (offset 57)
//     byte   luldIndicator;                     1 byte  (offset 58)
//     byte   nbboLuldIndicator;                 1 byte  (offset 59)
//     byte   nbboIndicator;                     1 byte  (offset 60)
//                                               total = 61 bytes

public class LongQuote
{
    // TODO: unsigned long problems?
    public static void currBlockMessageToQuote(TransmissionBlock block, Quote out)
    {
        int dataOffset = block.messageOffset() + TransmissionBlock.MSG_HEADER_SIZE;
        ByteBuffer buf = block.buffer();

        out.setType();
        out.setSymbol(buf, dataOffset, 11);
        out.setAskExchange(block.currParticipantID());
        out.setBidExchange(block.currParticipantID());
        out.setConditions(buf, dataOffset+12, 1);
        long op = buf.getLong(dataOffset+26);
        out.setAskPrice(op / 1e6);
        long bp = buf.getLong(dataOffset+14);
        out.setBidPrice(bp / 1e6);
        out.setAskSize(buf.getInt(dataOffset+34));
        out.setBidSize(buf.getInt(dataOffset+22));
        byte nbbo = buf.get(dataOffset+60) == (byte) 'G' ? (byte) 1 : (byte) 0;
        out.setNbbo(nbbo);
        // out.setTape(tape);
    }
}
