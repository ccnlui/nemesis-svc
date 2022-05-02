package nemesis.svc;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "nemesis",
    mixinStandardHelpOptions = true,
    description = "nemesis nanoservices",
    usageHelpAutoWidth = true
)
public class Nemesis {
    
    public static void main( String[] args ) {

        CommandLine cmd = new CommandLine(new Nemesis());
        cmd.addSubcommand("echoserver", EchoServer.class);
        cmd.addSubcommand("echoclient", EchoClient.class);
        cmd.addSubcommand("subscribe", Subscribe.class);
        System.exit(cmd.execute(args));

    }

}
