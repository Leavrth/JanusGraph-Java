package acekg.jgimporter;


import javax.xml.validation.Schema;

/**
 * Hello world!
 *
 */
public class App 
{
    /* args[0] : vertex -> vertexImporter
     *           edge -> edgeImporter
     * args[1] : propFileName
     * args[2] : dataFileName
     * args[3] : StringLabel
     */
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        if (args == null || args.length == 0) {
            System.out.println("缺少参数，通过-h可以查看帮助");
            return;
        }
        if (args.length == 1 && args[0] == "-h") {
            System.out.println("以下是必要参数：");
            System.out.println("\t-r\t:\t[vertex] 导入顶点");
            System.out.println("\t\t\t[edge] 导入边");
            System.out.println("\t-pF\t:\t[propFileName] JanusGraph 配置文件路径");
            System.out.println("\t-dF\t:\t[dataFileName] 导入数据文件路径");
            System.out.println("\t-l\t:\t[Label] 导入顶点模式的顶点label\n\n");
            System.out.println("可选参数：");
            System.out.println("\t-u\t:\t[user] mysql user");
            System.out.println("\t-p\t:\t[password] mysql password");
            System.out.println("\t-mH\t:\t[mysql host] mysql host");
            System.out.println("\t-mP\t:\t[mysql Port] mysql port");
            System.out.println("\t-d\t:\t[mysql database] mysql database");
            System.out.println("\t-rH\t:\t[redis host] redis host");
            System.out.println("\t-rP\t:\t[redis port] redis port");
            System.out.println("\t-s\t:\t[propFileName] [schemaFileName] step into schema importer");
            return;
        }
        if (args[0] == "-s") {
            if (args.length == 3)
                SchemaImporter.init(args[1], args[2]);
            return;
        }
        int checksum = 0;
        boolean runMode = true;     // checksum |= 1
        String propFileName = null; // checksum |= 2
        String dataFileName = null; // checksum |= 4
        String Label = null;        // checksum |= 8

        int checksumURL = 0;
        String database = null;     // checksumURL |= 1
        String host = null;         // checksumURL |= 2
        String port = null;         // checksumURL |= 4
        for (int i = 1; i < args.length; i+=2) {
            switch (args[i-1]) {
                case "-h":
                    System.out.println("通过-h可以查看帮助");
                    return;
                case "-r":
                    if (args[i].equals("vertex")) {
                        runMode = true;
                        checksum |= 1;
                    } else if (args[i].equals("edge")) {
                        runMode = false;
                        checksum |= 1;
                    } else {
                        System.out.println("error arg : " + args[i-1] + " " + args[i]);
                        return;
                    }
                    break;
                case "-pF":
                    propFileName = args[i];
                    checksum |= 2;
                    break;
                case "-dF":
                    dataFileName = args[i];
                    checksum |= 4;
                    break;
                case "-l":
                    Label = args[i];
                    checksum |= 8;
                    break;
                case "-u":
                    Importer.setUser(args[i]);
                    break;
                case "-p":
                    Importer.setPassword(args[i]);
                    break;
                case "-d":
                    database = args[i];
                    checksumURL |= 1;
                    break;
                case "-mH":
                    host = args[i];
                    checksumURL |= 2;
                    break;
                case "-mP":
                    port = args[i];
                    checksumURL |= 4;
                    break;
                case "-rH":
                    Importer.setHost(args[i]);
                    break;
                case "-rP":
                    Importer.setPort(Integer.parseInt(args[i]));
                    break;
                default:
                    System.out.println("error args : " + args[i-1] + "" + args[i]);
                    break;
            }
        }

        if (checksumURL == 7) {
            Importer.setUrl(host, port, database);
        }
        if (runMode && checksum == 15) {
            Importer.vertexImporter(propFileName, dataFileName, Label);
            return;
        }
        if (!runMode && checksum == 7) {
            Importer.edgeImporter(propFileName, dataFileName);
            return;
        }

        System.out.println("finished.");
    }
}
