package nemesis.svc.command;

import java.util.concurrent.Callable;

import nemesis.svc.nanoservice.Config;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "broadcaster",
    usageHelpAutoWidth = true,
    description = "start websocket broadcast server")
public class Broadcaster implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

    @Option(names = "--embedded-media-driver", defaultValue = "false",
        description = "launch with embedded media driver (default ${DEFAULT-VALUE})")
    boolean embeddedMediaDriver;

    @Option(names = "--aeron-dir", description = "override directory name for embedded aeron media driver")
    String aeronDir;

    @Option(names = "--sub-endpoint", description = "aeron udp transport endpoint from which messages are subscribed <address:port>")
    String subEndpoint;

    @Option(names = "--websocket-lib", description = "websocket server library")
    String websocketLib;

    @Option(names = "--port", description = "websocket server port")
    int websocketPort;

    @Override
    public Void call() throws Exception
    {
        mergeConfig();
        new nemesis.svc.nanoservice.Broadcaster().run();
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
        if (this.subEndpoint != null && !this.subEndpoint.isEmpty())
        {
            Config.subEndpoint = this.subEndpoint;
        }
        if (this.websocketPort != 0)
        {
            Config.websocketPort = this.websocketPort;
        }
        if (this.websocketLib != null && !this.websocketLib.isEmpty())
        {
            Config.websocketLib = this.websocketLib;
        }
    }
}
