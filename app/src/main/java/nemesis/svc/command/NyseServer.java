package nemesis.svc.command;

import java.util.concurrent.Callable;

import nemesis.svc.nanoservice.Config;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "nyse-server",
    usageHelpAutoWidth = true,
    description = "subscribe and server NYSE multicast marketdata")
public class NyseServer implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option(names = "--embedded-media-driver", defaultValue = "false",
        description = "launch with embedded media driver (default ${DEFAULT-VALUE})")
    boolean embeddedMediaDriver;

    @Option(names = "--aeron-dir", description = "directory name for aeron media driver")
    String aeronDir;

    @Option(names = "--pub-endpoint", description = "aeron udp transport endpoint to which messages are published <address:port>")
    String pubEndpoint;

    @Option(names = {"-i", "--interface"}, description = "network interface")
    String networkInterface;

    @Option(names = "--addr", description = "multicast group address to subscribe")
    String addr;

    @Option(names = "--port", description = "multicast group port to subscribe")
    int port;

    @Override
    public Void call() throws Exception
    {
        mergeConfig();
        new nemesis.svc.nanoservice.NyseServer().run();
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
        if (this.networkInterface != null && !this.networkInterface.isEmpty())
        {
            Config.networkInterface = this.networkInterface;
        }
        if (this.addr != null && !this.addr.isEmpty())
        {
            Config.addr = this.addr;
        }
        if (this.port != 0)
        {
            Config.port = this.port;
        }
    }
}
