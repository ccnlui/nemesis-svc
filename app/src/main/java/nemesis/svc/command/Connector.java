package nemesis.svc.command;

import java.util.concurrent.Callable;

import nemesis.svc.nanoservice.Config;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "connector",
    usageHelpAutoWidth = true,
    description = "subscribe to quote + trade messages and pipe to marshaller")
public class Connector implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option(names = "--embedded-media-driver", defaultValue = "false",
        description = "launch with embedded media driver (default ${DEFAULT-VALUE})")
    boolean embeddedMediaDriver;

    @Option(names = "--aeron-dir", description = "override directory name for embedded aeron media driver")
    String aeronDir;

    @Option(names = "--pub-endpoint", description = "aeron udp transport endpoint to which messages are published <address:port>")
    String pubEndpoint;

    @Option(names = "--sub-endpoint", description = "aeron udp transport endpoint from which messages are subscribed <address:port>")
    String subEndpoint;

    @Override
    public Void call() throws Exception
    {
        mergeConfig();
        new nemesis.svc.nanoservice.Connector().run();
        return null;
    }

    private void mergeConfig()
    {
        if (this.embeddedMediaDriver)
        {
            Config.embeddedMediaDriver = this.embeddedMediaDriver;
        }
        if (this.aeronDir != null && !this.aeronDir.isEmpty())
        {
            Config.aeronDir = this.aeronDir;
        }
        if (this.pubEndpoint != null && !this.pubEndpoint.isEmpty())
        {
            Config.pubEndpoint = this.pubEndpoint;
        }
        if (this.subEndpoint != null && !this.subEndpoint.isEmpty())
        {
            Config.subEndpoint = this.subEndpoint;
        }
    }
}
