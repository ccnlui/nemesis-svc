package nemesis.svc.message.cqs;

import java.nio.ByteBuffer;

import nemesis.svc.message.Trade;

//-----------------------------------------------------------------------------
// CTS_Pillar_Output_Specification 6.7.3
//-----------------------------------------------------------------------------
// class LongTrade
//     byte[] securitySymbol;                   11 bytes (offset 0)
//     byte   instrumentType;                    1 byte  (offset 11)
//     byte[] saleCondition;                     4 byte  (offset 12)
//     long   tradePrice;                        8 bytes (offset 16)
//     int    tradeVolume;                       4 bytes (offset 24)
//     byte   sellersSaleDays;                   1 byte  (offset 28)
//     byte   stopStockIndicator;                1 byte  (offset 29)
//     byte   tradeThroughExemptIndicator;       1 byte  (offset 30)
//     byte   tradeReportingFacilityID;          1 bytes (offset 31)
//     long   timestamp2;                        8 bytes (offset 32)
//     byte   shortSaleRestrictionIndicator;     1 byte  (offset 40)
//     byte   primaryListingMarketParticipantID; 1 byte  (offset 41)
//     byte   financialStatusIndicator;          1 byte  (offset 42)
//     byte   heldTradeIndicator;                1 byte  (offset 43)
//     byte   consolidatedHLLIndicator;          1 byte  (offset 44)
//     byte   participantOHLLIndicator;          1 byte  (offset 45)
//                                               total = 46 bytes
public class LongTrade
{
    // TODO: unsigned long problems?
    public static void currBlockMessageToTrade(TransmissionBlock block, Trade out)
    {
        int dataOffset = block.messageOffset() + TransmissionBlock.MSG_HEADER_SIZE;
        ByteBuffer buf = block.buffer();

        out.setType();
        out.setSymbol(buf, dataOffset, 11);
        out.setVolume(buf.getInt(dataOffset+24));
        out.setID(block.currParticipantReferenceNumber());
        out.setTimestamp(block.currTimestamp1());
        long tp = buf.getLong(dataOffset+16);
        out.setPrice(tp / 1e6);
        out.setConditions(buf, dataOffset+12, 4);
        out.setExchange(block.currParticipantID());
        // out.setTape(tape);;
    }
}
