package storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

import static utils.Constants.*;

/**
 * @author amirphl
 */
public class HBase {
    private int counter = 0;
    private ArrayList<Put> putObjects;
    public static HashSet<Integer> setOfInsertedLinksAsRow = new HashSet<>();
    private static Connection connection;
    private Table table;

    public HBase() throws IOException {
        putObjects = new ArrayList<>();
        if (connection == null) {
            Configuration config = HBaseConfiguration.create();
//            config.set("hbase.zookeeper.quorum", "server1"); //176.31.***.177
//            config.set("hbase.zookeeper.property.clientPort", "2181"); //2181
            connection = ConnectionFactory.createConnection(config);
        }
        table = connection.getTable(TableName.valueOf(TABLE_NAME));
    }

    public void insertLinks(String url, Map.Entry<String, String>[] linksPlusAnchors) throws IOException {
        if (linksPlusAnchors == null || linksPlusAnchors.length == 0) {
            return;
        }
        StringBuilder links = new StringBuilder();
        StringBuilder anchors = new StringBuilder();

        for (Map.Entry<String, String> e : linksPlusAnchors) {
            links.append(e.getKey());
            links.append(" ");
            if (e.getValue() == null || e.getValue().equals(""))
                anchors.append(" ");
            else
                anchors.append(e.getValue());
            anchors.append(SEPARATOR);
        }
        Put put = new Put(Bytes.toBytes(url));

        put.addColumn(Bytes.toBytes(FAMILY_NAME),
                Bytes.toBytes(COLUMN_QUALIFIER_NAME_1), Bytes.toBytes(links.toString()));
        put.addColumn(Bytes.toBytes(FAMILY_NAME),
                Bytes.toBytes(COLUMN_QUALIFIER_NAME_2), Bytes.toBytes(anchors.toString()));

        putObjects.add(put);
        counter++;
        //todo solve data loss by power off
        if (counter == HBASE_PUT_BATCH_NUMBER) {
            table.put(putObjects);
            counter = 0;
            putObjects = new ArrayList<>();
        }
        setOfInsertedLinksAsRow.add(url.hashCode());
    }

    public void checkAndReplace(Pair<String, String>[] linkAnchors) {
        for (int i = 0; i < linkAnchors.length; i++) {
            if (setOfInsertedLinksAsRow.contains(linkAnchors[i].getKey().hashCode()))
                linkAnchors[i] = null;
        }
    }

    public boolean exists(String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        return table.exists(get);
    }

    public void closeConnection() throws IOException {
        if (!connection.isClosed())
            synchronized (HBase.class) {
                connection.close();
            }
    }

    public void closeTable() throws IOException {
        table.close();
    }

    //todo test these too
    private void newMethods() throws IOException {
        BufferedMutator mutator = connection.getBufferedMutator(new BufferedMutatorParams(TableName.valueOf(TABLE_NAME)));
        mutator.mutate(new Put(Bytes.toBytes("row")));
        mutator.close();
    }

}