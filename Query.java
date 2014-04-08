import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Servlet implementation class Query1
 */
public class Query extends HttpServlet {
    private static final long serialVersionUID = 1L;

    /**
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
     */

    public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

    public static final String hbaseHost="localhost";
    public static final String hbaseZookeeperQuorum="localhost";
    public static final String hbaseZookeeperClientPort="2181";
    public static final String tableName="ReTweet";

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        out.println("HowIMetYourCloud,5820-6355-2921");
        // Q4 Specific
        String userid = request.getParameter("userid");
        //String userid = request.getParameter("23344444444");
        if (userid == null)
            return ;
        out.println("HowIMetYourCloud,5820-6355-vinoth");
        
        Configuration conf = HBaseConfiguration.create();

        conf.setInt("timeout", 120000);
        conf.set("hbase.master", "*" + hbaseHost + ":9000*");
        conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hbaseZookeeperQuorum);
        conf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, hbaseZookeeperClientPort);

        @SuppressWarnings("resource")
        HTable table = new HTable(conf, "Tweet");

        Filter filter = new PrefixFilter(Bytes.toBytes(userid));

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
          for (KeyValue kv : result.raw()) {
              out.println("ReTweet Users: " + Bytes.toString(kv.getValue()));
          }
        }
        scanner.close();


    }


    /**
     * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // TODO Auto-generated method stub

    }

}
