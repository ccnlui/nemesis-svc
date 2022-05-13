package nemesis.svc.agent;

import io.aeron.Publication;
import io.aeron.exceptions.AeronException;

final class AgentUtil
{
    static boolean retryPublicationResult(final long result)
    {
        if (result == Publication.ADMIN_ACTION ||
            result == Publication.BACK_PRESSURED)
        {
            return true;
        }
        else if (result == Publication.CLOSED || 
            result == Publication.MAX_POSITION_EXCEEDED ||
            result == Publication.NOT_CONNECTED)
        {
            throw new AeronException("Publication error: " + Publication.errorString(result));
        }
        return false;
    }
}
