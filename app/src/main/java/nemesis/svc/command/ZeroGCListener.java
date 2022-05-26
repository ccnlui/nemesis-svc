package nemesis.svc.command;


import java.util.concurrent.Callable;

import nemesis.svc.nanoservice.Config;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "zgc-listener",
    usageHelpAutoWidth = true,
    description = "subscribe to a single multicast marketdata feed without generating garbage")
public class ZeroGCListener implements Callable<Void>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "help message")
    boolean help;

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
        new nemesis.svc.nanoservice.ZeroGCListener().run();
        return null;
    }

    private void mergeConfig()
    {
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
