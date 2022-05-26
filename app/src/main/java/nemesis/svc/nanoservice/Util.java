package nemesis.svc.nanoservice;

import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

final class Util
{
    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    private Util()
    {
    }

    static MediaDriver launchEmbeddedMediaDriverIfConfigured()
    {
        if (Config.embeddedMediaDriver)
        {
            MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .threadingMode(ThreadingMode.DEDICATED)
                .conductorIdleStrategy(new BusySpinIdleStrategy())
                .senderIdleStrategy(new NoOpIdleStrategy())
                .receiverIdleStrategy(new NoOpIdleStrategy())
                .dirDeleteOnShutdown(true);
            if (Config.aeronDir != null && !Config.aeronDir.isEmpty())
            {
                mediaDriverCtx = mediaDriverCtx.aeronDirectoryName(Config.aeronDir);
            }
            MediaDriver md = MediaDriver.launchEmbedded(mediaDriverCtx);
            LOG.info(mediaDriverCtx.toString());
            return md;
        }
        return null;
    }

    static Aeron connectAeron(MediaDriver mediaDriver)
    {
        Aeron.Context aeronCtx = new Aeron.Context().idleStrategy(new NoOpIdleStrategy());
        if (mediaDriver != null)
        {
            aeronCtx = aeronCtx.aeronDirectoryName(mediaDriver.aeronDirectoryName());
        }
        else if (Config.aeronDir != null && !Config.aeronDir.isEmpty())
        {
            aeronCtx = aeronCtx.aeronDirectoryName(Config.aeronDir);
        }
        LOG.info(aeronCtx.toString());
        return Aeron.connect(aeronCtx);
    }

    static String aeronIpcOrUdpChannel(String endpoint)
    {
        if (endpoint == null || endpoint.isEmpty())
            return "aeron:ipc";
        else
            return "aeron:udp?endpoint=" + endpoint + "|mtu=1408";
    }

    static void closeIfNotNull(final AutoCloseable closeable) throws Exception
    {
        if (closeable != null)
            closeable.close();
    }
}
