
import java.sql.* ;
import java.util.*;
import java.io.*;
public class DBInit {

  private Statement db_stmt;

  public void connectDB(String my_db_name){
    try{
      Class.forName("com.mysql.jdbc.Driver");
      String url = "jdbc:mysql://localhost:3306/"+my_db_name + "?useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
      Connection con = DriverManager.getConnection(url, "root", "123456");
    //  Connection con = DriverManager.getConnection(url, "root", "qwerty");
      if(con == null)
        System.out.println("CANNOT ESTABLISH CONNECTION");
      db_stmt = con.createStatement();
    }catch(SQLException ex){
      ex.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (Exception ex) {
        // handle the error
    }
  }

  public void createDBTablesOfTracker(){
    try {

      db_stmt.execute("DROP TABLE IF EXISTS Peers;");
      db_stmt.execute("DROP TABLE IF EXISTS FileChunks;");
      db_stmt.execute("DROP TABLE IF EXISTS ChunksPeers;");
      db_stmt.execute("DROP TABLE IF EXISTS FileSizes;");


      db_stmt.executeUpdate("CREATE TABLE Peers(peer_id varchar(100) PRIMARY KEY, peer_ip	varchar(100), peer_port INT) ENGINE = InnoDB;");
      db_stmt.executeUpdate("CREATE TABLE FileChunks(file_name varchar(100), chunk_name	varchar(100), PRIMARY KEY (file_name, chunk_name)) ENGINE = InnoDB;");
      db_stmt.executeUpdate("CREATE TABLE ChunksPeers(chunk_name varchar(100), peer_id varchar(100), PRIMARY KEY (chunk_name, peer_id)) ENGINE = InnoDB;");
      db_stmt.executeUpdate("CREATE TABLE FileSizes(file_name varchar(100) PRIMARY KEY, file_size DOUBLE) ENGINE = InnoDB;");

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void createDBTablesOfPeer(String foldername){
    try {

      db_stmt.execute("DROP TABLE IF EXISTS FileChunk;");
      db_stmt.executeUpdate("CREATE TABLE FileChunk(filename varchar(100), chunkname	varchar(100), register_flag INT, PRIMARY KEY (filename, chunkname)) ENGINE = InnoDB;");
      writeTable(foldername);

    } catch (SQLException e) {
      e.printStackTrace();
    }

  }

  public void writeTable(String foldername) {

    ArrayList<String> results = new ArrayList<String>();


    File[] files = new File(foldername).listFiles();

    for (File file : files) {
      if (file.isFile()) {
        results.add(file.getName());
      }
    }


    for (int i = 0; i < results.size(); i++)
    {
      String first_part = results.get(i).substring(0, results.get(i).indexOf("_")-1);
      if(first_part.compareTo("file") == 0)
      {
        String file_index = results.get(i).substring(results.get(i).indexOf("_")-1, results.get(i).length());
        System.out.println("First Part: "+first_part + ", file index: " + file_index);


        for (int j = 0; j < results.size(); j++)
        {
          String curfilename = (results.get(j).substring(0, results.get(j).indexOf("_")-1));
          String curfileindex = results.get(j).substring(results.get(j).indexOf("_")-1, results.get(j).length()-2);
          System.out.println(curfilename + " index: "+ curfileindex);
          if(curfilename.compareTo("chunk") == 0 && curfileindex.compareTo(file_index) == 0)
          {
            System.out.println("chunkk: "+results.get(j));
            String query = "insert into FileChunk values (\"" + results.get(i) + "\", \"" + results.get(j) + "\", " + 0 + ");";
            try {
              db_stmt.executeUpdate(query);
            }catch (SQLException e){
              e.printStackTrace();
            }
          }
        }
      }

      
    }

  }


  public static void main(String args[]) {

    int tracker_or_peer = Integer.parseInt(args[0]);
    DBInit di = new DBInit();
    if(tracker_or_peer == 1){
      di.connectDB(args[1]);
      di.createDBTablesOfTracker();

    }
    else{
      di.connectDB(args[1]);
      di.createDBTablesOfPeer(args[2]);
    }
  }
}
