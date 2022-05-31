package nemesis.svc.message.cqs;

import java.nio.ByteBuffer;

import nemesis.svc.message.Trade;

//-----------------------------------------------------------------------------
// CTS_Pillar_Output_Specification 6.7.3
//-----------------------------------------------------------------------------
// class ShortTrade
//     byte[] securitySymbol;                    5 bytes (offset 0))
//     byte   saleCondition;                     1 byte  (offset 5)
//     byte   saleConditionCategory;             1 byte  (offset 6)
//     short  tradePrice;                        2 bytes (offset 7)
//     short  tradeVolume;                       2 bytes (offset 9))
//     byte   primaryListingMarketParticipantID; 1 byte  (offset 11)
//     byte   consolidatedHLLIndicator;          1 byte  (offset 12)
//     byte   participantOHLLIndicator;          1 byte  (offset 13)
//                                               total = 14 bytes
public class ShortTrade
{
    // TODO: unsigned long problems?
    public static void currBlockMessageToTrade(TransmissionBlock block, Trade out)
    {
        int dataOffset = block.messageOffset() + TransmissionBlock.MSG_HEADER_SIZE;
        ByteBuffer buf = block.buffer();

        out.setType();
        out.setSymbol(buf, dataOffset, 5);
        out.setVolume(buf.getShort(dataOffset+9));
        out.setID(block.currParticipantReferenceNumber());
        out.setTimestamp(block.currTimestamp1());
        short tp = buf.getShort(dataOffset+16);
        out.setPrice(tp / 1e2);
        out.setConditions(buf, dataOffset+5, 1);
        out.setExchange(block.currParticipantID());
        // out.setTape(tape);;
    }
}
