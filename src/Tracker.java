import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.io.File;
import java.rmi.Naming;
//import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;
import java.sql.* ;

public class Tracker extends Thread   implements ITracker { //

  String peer_IP = "";
  int peer_port = 0;
  int peer_cnt = 0;
  int epoch = 0;
  Map<String, Double> file_list; //file id, size of file
  ArrayList<NodeStruct> peers_list; //peer ip, peer port
  Map<String, Integer> heartbeat_peers;
  Map<String, ArrayList<String>> file_chunks; //file id, chunk ids
  Map<String, ArrayList<NodeStruct>> chunk_ids; //chunk id, peer id
  private Statement db_stmt;
  String my_db_name;
  //ReentrantLock lock;
  public Tracker() {
    file_list = new HashMap<String, Double>();
    peers_list = new ArrayList<NodeStruct>();
    file_chunks = new HashMap<String, ArrayList<String>>();
    chunk_ids = new HashMap<String, ArrayList<NodeStruct>>();
    heartbeat_peers = new HashMap<String, Integer>();
	//lock = new ReentrantLock();

  }


    public void connectDB(String my_db_name){
      try{
      Class.forName("com.mysql.jdbc.Driver");
    	//Class.forName("com.mysql.jdbc.Driver");
        String url = "jdbc:mysql://localhost:3306/"+my_db_name + "?useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
        Connection con = DriverManager.getConnection(url, "root", "123456");
        //Connection con = DriverManager.getConnection(url, "root", "qwerty");
        db_stmt = con.createStatement();
      }catch(SQLException ex){
        ex.printStackTrace();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    }

    /*public void createDBTables(){
      try {

        db_stmt.execute("DROP TABLE IF EXISTS Peers;");
        db_stmt.execute("DROP TABLE IF EXISTS FileChunks;");
        db_stmt.execute("DROP TABLE IF EXISTS ChunksPeers;");

        db_stmt.executeUpdate("CREATE TABLE Peers(peer_id varchar(100) PRIMARY KEY, peer_ip	varchar(100), peer_port INT) ENGINE = InnoDB;");
        db_stmt.executeUpdate("CREATE TABLE FileChunks(file_name varchar(100), chunk_name	varchar(100), PRIMARY KEY (filename, chunk_name)) ENGINE = InnoDB;");
        db_stmt.executeUpdate("CREATE TABLE ChunksPeers(chunk_name varchar(100), peer_id varchar(100), PRIMARY KEY (chunk_name, peer_id)) ENGINE = InnoDB;");

      } catch (SQLException e) {
        e.printStackTrace();
      }
    }*/

    public void insertToPeers(NodeStruct node)
    {
      String query = "insert into Peers values (\"" + node.peer_id + "\", \"" + node.peer_ip + "\", " + node.peer_port + ");";
    	try {
    		db_stmt.executeUpdate(query);
    	}catch (SQLException e){
  			e.printStackTrace();
  		}
    }

    public void insertToFileSizes(String file_name, double file_size)
    {
      String query = "insert into FileSizes values (\"" + file_name + "\", " +file_size +  ");";
    	try {
    		db_stmt.executeUpdate(query);
    	}catch (SQLException e){
  			e.printStackTrace();
  		}
    }

    public void insertToFileChunks(String file_name, String chunk_name)
    {
      System.out.println("INSERT TO FILE CHUNKS " + file_name + " chunkname: " + chunk_name);
      String query = "insert into FileChunks values (\"" + file_name + "\", \"" + chunk_name + "\");";
    	try {
    		db_stmt.executeUpdate(query);
    	}catch (SQLException e){
  			e.printStackTrace();
  		}
    }

    public void insertToChunksPeers(String chunk_name, String peer_id)
    {
      String query = "insert into ChunksPeers values (\"" + chunk_name + "\", \"" + peer_id + "\");";
    	try {
    		db_stmt.executeUpdate(query);
    	}catch (SQLException e){
  			e.printStackTrace();
  		}
    }

    public void readFileChunks()
    {
      String query = "select * from FileChunks";
      System.out.println("IN READ FILE CHUNKS");

      try{
        ResultSet rset = db_stmt.executeQuery(query);
        while(rset.next()) {
          System.out.println("FILECHUNKS TABLE IS NOT EMPTY");
            String cur_filename = rset.getString("file_name");
            File file = new File(cur_filename);
            double megabytes = (file.length()/1024)/1024;
            if(!file_list.containsKey(cur_filename)){
              file_list.put(cur_filename, megabytes);
              ArrayList<String> chunks = new ArrayList<String>();
              chunks.add(rset.getString("chunk_name"));
              file_chunks.put(cur_filename, chunks);
              System.out.println("put to file_chunks " + cur_filename);

            }
            else{
              file_chunks.get(cur_filename).add(rset.getString("chunk_name"));
            }
        }
      }catch (SQLException e){
        e.printStackTrace();
      }
    }

    public void readPeers()
    {
      String line = null;

      String query = "select * from Peers";
      try{
        ResultSet rset = db_stmt.executeQuery(query);
        while(rset.next()) {
            NodeStruct n = new NodeStruct(rset.getString("peer_id"), rset.getString("peer_ip"), Integer.parseInt(rset.getString("peer_port")) );
            peers_list.add(n);
        }
      }catch (SQLException e){
        e.printStackTrace();
      }
    }

    public void readFileSizes()
    {
      String line = null;

      String query = "select * from FileSizes";
      try{
        ResultSet rset = db_stmt.executeQuery(query);
        while(rset.next()) {

            file_list.put(rset.getString("file_name"), rset.getDouble("file_size"));
        }
      }catch (SQLException e){
        e.printStackTrace();
      }
    }

    public void readChunksPeers()
    {
      String query = "select * from ChunksPeers INNER JOIN Peers ON ChunksPeers.peer_id = Peers.peer_id;";
      try{
        ResultSet rset = db_stmt.executeQuery(query);
        while(rset.next()) {
            String chname = rset.getString("chunk_name");
            String id = rset.getString("peer_id");
            String ip = rset.getString("peer_ip");
            int port = rset.getInt("peer_port");
            NodeStruct n = new NodeStruct(id, ip, port);
            if(!chunk_ids.containsKey(chname)){
              ArrayList<NodeStruct> ids = new ArrayList<NodeStruct>();
              ids.add(n);
              //DATABASE

              chunk_ids.put(chname, ids);
            }
            else{
              //DATABASE

              chunk_ids.get(chname).add(n);
            }
        }
      }catch (SQLException e){
        e.printStackTrace();
      }
    }

    public boolean registerReply(NodeStruct node, int num_files, Map<String, Double> files, Map<String, ArrayList<String>> fc) throws RemoteException {

        peer_cnt++;
        peers_list.add(node);
        insertToPeers(node);
        System.out.println("Registered new peer - " + node.peer_id);
        for (Map.Entry<String,Double> entry : files.entrySet()) {
        	String file_name = entry.getKey();
        	double size = entry.getValue();
	          file_list.put(file_name, size );
	          insertToFileSizes(file_name, size);

        }
        for (Map.Entry<String,ArrayList<String>> entry : fc.entrySet()) {
        	String filename = entry.getKey();
        	ArrayList<String> chunks = entry.getValue();
	          file_chunks.put(filename, chunks);
	          System.out.println("chunks filename: " + filename + " CHNKS SIZE: "+chunks.size());
	          for(String chunkname: chunks) {
              System.out.println("**************************************abcd");
	            insertToFileChunks(filename, chunkname);
	          }
        }
        for (Map.Entry<String,ArrayList<String>> entry : fc.entrySet()) {
          for(int i = 0; i < entry.getValue().size(); i++)
          {
            String chunkname = entry.getValue().get(i);
            if(chunk_ids.containsKey(chunkname))
            {
              chunk_ids.get(chunkname).add(node);
            }
            else
            {
              ArrayList<NodeStruct> ids = new ArrayList<NodeStruct>();
              ids.add(node);
              chunk_ids.put(chunkname, ids);
            }
            insertToChunksPeers(chunkname, node.peer_id);

          }
        }


        return true;
      }


    public Map<String, Double> fileListReply(){
      return file_list;
    }

    public Map<String, ArrayList<NodeStruct>> FileLocationsReply(String req_file){
      Map<String, ArrayList<NodeStruct>> ret = new HashMap<String, ArrayList<NodeStruct>>();
      if(!file_chunks.isEmpty()) {
        System.out.println(file_chunks.size());
    	  ArrayList<String> chunkss = file_chunks.get(req_file);
          System.out.println("req filename: " + req_file);

          if(chunkss != null) {
			      if(!chunkss.isEmpty()) {
              System.out.println("CHUNKS IS NOT EMPTY");
              for(int i = 0; i < chunkss.size(); i++)
              {
                String cur_chunk_name = chunkss.get(i);
                ret.put(cur_chunk_name, chunk_ids.get(cur_chunk_name));
              }
			      }
          }
      }

      System.out.println("Sent Chunk locations list for " + req_file );
      return ret;
    }
    public boolean chunkRegisterReply(NodeStruct peer, String chunk_id) {
    	//  update chunk_ips
    	// file_list and file_chunk should already have the file
    	//chunk_ips surely has the chunk_id key

    	chunk_ids.get(chunk_id).add(peer);
    	System.out.println("Received new chunk " + chunk_id + " from " + peer.peer_id);
	      insertToChunksPeers(chunk_id, peer.peer_id);
	      return true;
    }

    public boolean leaveReply(NodeStruct peer) {

    	//delete from chunk_ips
    	System.out.println("chunkids: " + chunk_ids.size() );
    	System.out.println("chunkfile: " + file_chunks.size() );
    	System.out.println("peers: " + peers_list.size() );

    	//delete peer location for a chunk
    	for (Map.Entry<String,ArrayList<NodeStruct>> entry : chunk_ids.entrySet()) {
    		ArrayList<NodeStruct> nodes = entry.getValue();
    		Iterator<NodeStruct> itr = nodes.iterator();
    		while(itr.hasNext()) {
    			NodeStruct n = itr.next();
    			if(n.peer_id.compareTo(peer.peer_id) == 0) {
    				itr.remove();
    				//String del_chunkname = entry.getKey();
    				String query = "DELETE FROM ChunksPeers WHERE peer_id = \"" + n.peer_id + "\";";
    		    	try {
    		    		db_stmt.executeUpdate(query);
    		    		System.out.println("delete from chunkspeers execute");
    		    	}catch (SQLException e){
    		  			e.printStackTrace();
    		  		}
    			}
    		}
    	}

    	//delete chunk with no locations;

    	Iterator<Entry<String, ArrayList<NodeStruct>>> entryIts = chunk_ids.entrySet().iterator();

    	while(entryIts.hasNext()) {
    		Entry<String, ArrayList<NodeStruct>> en = entryIts.next();
    		ArrayList<NodeStruct> nodes = en.getValue();
    		if(nodes.isEmpty()) {
    			entryIts.remove();
    			String del_chunkname = en.getKey();
    			Iterator<Entry<String, ArrayList<String>>> fcentryits = file_chunks.entrySet().iterator();

		    	while (fcentryits.hasNext()) {
		    		Entry<String, ArrayList<String>> fcentry = fcentryits.next();
		    		Iterator<String> centry = fcentry.getValue().iterator();
		    		while(centry.hasNext()) {
		    			String chunkname = centry.next();
		    			if(del_chunkname.compareTo(chunkname) == 0) {
		    				centry.remove();
		    				String query = "DELETE FROM  FileChunks WHERE chunk_name = \"" + chunkname + "\";";
		    		    	try {
		    		    		db_stmt.executeUpdate(query);
		    		    		System.out.println("delete from filechunks execute");

		    		    	}catch (SQLException e){
		    		  			e.printStackTrace();
		    		  		}
		    			}
		    		}
		    	}
    		}
    	}

    	//delete file which doesnt have a chunk


    	Iterator<Entry<String, ArrayList<String>>> fentryIts = file_chunks.entrySet().iterator();

    	while (fentryIts.hasNext()) {
    		Entry<String, ArrayList<String>> entry = fentryIts.next();
    		if (entry.getValue().isEmpty()) {
        		String file_name = entry.getKey();
    			fentryIts.remove();
    			file_list.remove(file_name);

		    	String query = "DELETE FROM  FileSizes WHERE file_name = \"" + file_name + "\";";
		    	try {
		    		db_stmt.executeUpdate(query);
		    		System.out.println("delete from filesizes execute");

		    	}catch (SQLException e){
		  			e.printStackTrace();
		  		}
    		}
    	}
      return true;
    }

    public boolean heartBeat(NodeStruct node) throws RemoteException {
        System.err.println("Received Heart Beat from " + node.peer_id);
        heartbeat_peers.put(node.peer_id, epoch);
        return true;
    }

    public void run() {
        try {
            while (true) {
            	System.out.println( "sz "+ heartbeat_peers.size());
        		Iterator<Entry<String, Integer>> itr = heartbeat_peers.entrySet().iterator();

            	while (itr.hasNext()) {
            		Entry<String, Integer> e = itr.next();
            		System.out.println("epoch: " + e.getKey() + " " + (epoch - e.getValue()));

            		if((epoch - e.getValue()) > 2) {
            			//peer died
            			String peer_id = e.getKey();
            			Iterator<NodeStruct> pitr = peers_list.iterator();
            	    	while(pitr.hasNext()) {
            	    		NodeStruct n = pitr.next();
            	    		if(n.peer_id.compareTo(peer_id) == 0) {
            	    			System.out.println("DELETING " + peer_id);
                    			leaveReply(n);
                    			pitr.remove();
                    			String query = "DELETE FROM Peers WHERE peer_id = \"" + n.peer_id + "\";";
                		    	try {
                		    		db_stmt.executeUpdate(query);
                		    	}catch (SQLException eq){
                		  			eq.printStackTrace();
                		  		}
                    			itr.remove();
                    			break;
            	    		}
            	    	}
            		}

            	}

        		for( Map.Entry<String, Integer> entry : heartbeat_peers.entrySet()  ) {
            		System.out.println("epoch: " + entry.getKey() + " " + (epoch - entry.getValue()));

            	}
            	 Thread.sleep(30000);
            	//make all values false;
            	epoch++;
	         }

        } catch ( InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {

      try {
          System.setProperty("java.security.policy","security.policy");

        Tracker obj = new Tracker();
        ITracker stub = (ITracker) UnicastRemoteObject.exportObject(obj, 2014);

        // Bind the remote object's stub in the registry
        //Registry registry = LocateRegistry.createRegistry(3030);
        Registry registry = LocateRegistry.createRegistry(2014);
        registry.rebind("ITracker", stub);
        System.setProperty("java.rmi.server.hostname", "127.0.0.1");
    	/*String url = "rmi://localhost:1099/ITracker" ;
        Naming.rebind(url, stub);*/

        System.err.println("Server ready");
        //testing

        obj.connectDB(args[0]);
        System.out.println("Connected to Database: " + args[0]);
        obj.readPeers();
        obj.readFileChunks();
        obj.readChunksPeers();
        obj.readFileSizes();

        obj.start();




      } catch (Exception e) {
        System.err.println("Server exception: " + e.toString());
        e.printStackTrace();
      }
    }

  }
