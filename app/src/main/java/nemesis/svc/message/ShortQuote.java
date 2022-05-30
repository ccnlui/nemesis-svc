package nemesis.svc.message;

//-----------------------------------------------------------------------------
// CQS_Pillar_Output_Specification 6.4.3
//-----------------------------------------------------------------------------
// class ShortQuote
//     byte[] securitySymbol;                    1 bytes (offset 0)
//     short  bidPrice;                          2 bytes (offset 5)
//     short  bidSize;                           2 bytes (offset 7)
//     short  offerPrice;                        2 bytes (offset 9)
//     short  offerSize;                         2 bytes (offset 11)
//     byte   primaryListingMarketParticipantID; 1 byte  (offset 13)
//     byte   nbboIndicator;                     1 byte  (offset 14)
//                                               total = 15 bytes
public class ShortQuote
{
}
