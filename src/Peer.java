import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.Random; 
import java.util.List; 

public class Peer implements IPeer{

    final public static int BUF_SIZE = 1024 * 64;
    int sleep_seconds = 0;
    String my_db_name;
    String current_requested_file = "";
    String myfiles_name;
    String my_IP = "";
    String tracker_IP = "";
    String my_directory = "";
    String my_id = "";
    int my_db_port  = 0;
    int my_port = 0;
    int tracker_port = 0;
    Registry tracker_registry;
    boolean req_flag = false;
    static ITracker tracker_stub;
    private Statement db_stmt;
    static NodeStruct my_node; //stores my ip, my name and my port
    String is_rarest;
    Map<String, Double> my_file_list; //file id, size of files
    Map<String, Double> all_file_list; //file id, size of files
    Map<String, ArrayList<String>> file_chunks; //file id, chunk ids
    Map<String, ArrayList<NodeStruct>> req_file_chunk_ids; // chunk, peer ids
    Queue<QueueStruct> file_req_q;
	ArrayList<Pair> sz_chunks;
	ArrayList<String> friends;
	List<Request> requests;

    private Peer(String id, String ip, int port, String request_file, String storage_file, String tip, int tport, String db_name, String dir_name, int dbport, String rar, String frnd1_id, String frnd2_id) {
    	my_IP = ip;
    	my_port = port;
    	current_requested_file = request_file;
    	myfiles_name = storage_file;
    	tracker_IP = tip;
    	tracker_port = tport;
    	my_directory = dir_name;
    	my_id = id;
    	my_db_port = dbport;
    	is_rarest = rar;
    	
      my_db_name = db_name;
	  my_node = new NodeStruct(my_id, my_IP, my_port);

	  
      	my_file_list = new HashMap<String, Double>();
      	all_file_list = new HashMap<String, Double>();
      	file_chunks = new HashMap<String, ArrayList<String>>();
      	req_file_chunk_ids = new HashMap<String, ArrayList<NodeStruct>>();
      	file_req_q = new LinkedList<QueueStruct>();
      	requests = new ArrayList<Request>();
    	sz_chunks = new ArrayList<Pair>(); 
		friends = new ArrayList<String>();
	  	friends.add(frnd1_id);
	  	friends.add(frnd2_id);
      	//tracker_stub = null;
      	try {
            System.setProperty("java.security.policy","security.policy");
    		tracker_registry = LocateRegistry.getRegistry(tracker_IP, tracker_port);
        tracker_stub = (ITracker) tracker_registry.lookup("ITracker");

        } catch (Exception e) {
            System.err.println("peer exception " + e.toString());
            e.printStackTrace();
        }
    }
    
	public String getLookupName(String ip,int port,  String obj){
		String s="";
		s+="rmi://";
		s+=ip;
		s+=":";
		s+=Integer.toString(port);
		s+="/";
		s+=obj;
		return s;
	}

    public void connectDB(){
        try{
          //Class.forName("com.mysql.cj.jdbc.Driver");
		  Class.forName("com.mysql.jdbc.Driver");
		  System.out.println("db name: " +  my_db_name);
          String url = "jdbc:mysql://localhost:"+ my_db_port +"/"+my_db_name + "?useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
          //Connection con = DriverManager.getConnection(url, "root", "gulsumgudukbay");
          Connection con = DriverManager.getConnection(url, "root", "123456");
          db_stmt = con.createStatement();
        }catch(SQLException ex){
          ex.printStackTrace();
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
        }
      }
    // read table , fill my_file_list and file_chunks;
    public void readTable() {
    	String line = null;

  		String query = "select * from FileChunk";
  		try{
    		ResultSet rset = db_stmt.executeQuery(query);
    		while(rset.next()) {
	          String cur_filename = rset.getString("filename");
              String path_filename = my_directory + "/" + cur_filename;
	          File file = new File(path_filename);
	          double megabytes = (file.length());///1024);/1024;
              System.out.println(cur_filename + ", " + megabytes);
	          if(!my_file_list.containsKey(cur_filename)){
	            my_file_list.put(cur_filename, megabytes);
	            ArrayList<String> chunks = new ArrayList<String>();
	            chunks.add(rset.getString("chunkname"));
	            file_chunks.put(cur_filename, chunks);
	          }
	          else{
	            file_chunks.get(cur_filename).add(rset.getString("chunkname"));
	          }
    		}
  		}catch (SQLException e){
  			e.printStackTrace();
  		}
    }

    public void updateTable(String filename, String chunkname, int flag) {
    	String query = "update FileChunk SET register_flag = " + flag + " WHERE filename = \"" + filename + "\" and chunkname = \"" + chunkname + "\";";
    	try {
            System.out.println("executeUpdate: " + query);
    		db_stmt.executeUpdate(query);
    	}catch (SQLException e){
  			e.printStackTrace();
  		}
    }

    public void writeTable(String filename, String chunkname, int flag) {
    	String query = "insert into FileChunk values (\"" + filename + "\", \"" + chunkname + "\", " + flag + ");";
    	try {
    		db_stmt.executeUpdate(query);
    	}catch (SQLException e){
  			e.printStackTrace();
  		}
    }

    //DONE
    public void registerRequestToServer() {
    	//peer registers to tracker
        
    	try {
			tracker_stub.registerReply(my_node, my_file_list.size(), my_file_list, file_chunks);

            // updateTable
            System.out.println("Updating registered files");
            for( Map.Entry<String, ArrayList<String>> fc : file_chunks.entrySet() ){
                String filename = fc.getKey();
                for(String chunkname : fc.getValue() ){
                    updateTable(filename, chunkname, 1);
                }
                
            }
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }

    //done
    public void fileListRequest() {
    	//to get all files
      try{
        all_file_list = tracker_stub.fileListReply();
        System.out.println("all file list = " + all_file_list.size());
      }catch(RemoteException e)
      {
        e.printStackTrace();
      }
    }

    //done
    public boolean fileLocationsRequest() {
    	//to get file file locations
      try{
		  System.out.println("Requesting file locations .....");
        req_file_chunk_ids = tracker_stub.FileLocationsReply(current_requested_file);
        if(req_file_chunk_ids != null && !req_file_chunk_ids.isEmpty()) {
        	System.out.println("req_file_chunk_ids = " + req_file_chunk_ids.size());
        	return true;
        }
        else return false;
      }catch(RemoteException e)
      {
        e.printStackTrace();
      }
      return false;

    }

    //need to have a function that receives new chunks

    public void chunkRegisterRequest(String filename, String chunkname) {
    	//tells the tracker that it received a new chunk and registers
    	// assume  unregistered chunks is updated
    	//add chunk to my files list  and also send to tracker
    	if(my_file_list.containsKey(filename)){
    		double size = my_file_list.get(filename);
    		File file = new File(chunkname);
	        double megabytes = size + (file.length()/1024)/1024;
	        my_file_list.put(filename, megabytes);
    	}else {
    		File file = new File(chunkname);
	        double megabytes = (file.length()/1024)/1024;
    		my_file_list.put(filename, megabytes);


    	}

    	if(file_chunks.containsKey(filename)){
    		 file_chunks.get(filename).add(chunkname);
    	}else {
    		ArrayList<String> chunks = new ArrayList<String>();
    		chunks.add(chunkname);
    		file_chunks.put(filename, chunks);
    	}
      try{
    	   boolean recv = tracker_stub.chunkRegisterReply(my_node, chunkname);
       }catch(RemoteException e)
       {
         e.printStackTrace();
       }

    	updateTable(filename, chunkname, 1);
    	
    	System.out.println("registered new chunk " + chunkname + " from " + filename);
    }

    public void leaveRequest() {
    	//tells the server to remove this peer and all its files
      try{

    	   tracker_stub.leaveReply(my_node);
      }catch(RemoteException e)
      {
        e.printStackTrace();
      }
    }
    
    public void peerFileRecv(NodeStruct peer, String src_file, String dest_file) {
        
        try {
        	//String url = "rmi://" + peer.peer_ip + ":" + peer.peer_port + "/" + peer.peer_id;
        	//peer_stub = (IPeer) Naming.lookup(url);
        	Registry peer_registry = LocateRegistry.getRegistry(peer.peer_ip,peer.peer_port);
        	IPeer peer_stub = (IPeer) peer_registry.lookup(peer.peer_id);
        	/*byte[] br = peer_stub.peerFileSend(peer, src_file);
        
        	InputStream in = new ByteArrayInputStream(br);
            byte[] b = new byte[1024];
            //in.read(buffer);
            int len;
            String dest_file2 = my_directory + "/" + dest_file;
        	File dest = new File(dest_file2);
            OutputStream out = new FileOutputStream(dest);
            while ((len = in.read(b)) >= 0) {
                out.write(b, 0, len);
            }*/
        	QueueStruct qs = new QueueStruct(my_node, src_file);
            System.out.println("SOURCE FILE NAME: " + src_file);
        	byte[] br = peer_stub.peerFileSend(my_node, src_file);
            System.out.println("BR LENGTH: "+br.length);
            String dest_file2 = my_directory + "/" + dest_file;
            System.out.println("DST FILE NAME: " + dest_file2);
        	File dest = new File(dest_file2);
            OutputStream out = new FileOutputStream(dest);

            System.out.println("Downloading..." + dest_file2);
            for(int i = 0; i < br.length; i++) {
            	System.out.print("-");
            }
            System.out.print("\n");

            for(int i = 0; i < br.length; i++) {
            	out.write(br, i, 1);
            	TimeUnit.MILLISECONDS.sleep(15);;
            	System.out.print("-");
            }
            System.out.print("\n");
            //out.write(br);
            /*            byte[] b = new byte[1024];

             * int len;

            while ((len = br.read(b)) >= 0) {
                out.write(b, 0, len);
            }*/
            System.out.println("received chunk " + dest_file + " from " + peer.peer_id);
            out.close();
            chunkRegisterRequest(current_requested_file, dest_file);
			//TODO inserttotable;
			writeTable(current_requested_file, dest_file, 1);
        }catch (MalformedURLException e1) {
			e1.printStackTrace();
		}catch (RemoteException e1) {
			e1.printStackTrace();
		}catch (NotBoundException e) {
			e.printStackTrace();
		}
        catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
            
	} 
    
    
    public byte[] peerFileSend(NodeStruct peer, String src_file) {
    	ByteArrayOutputStream bout = null;
    	
            try {
            	if(friends.contains(peer.peer_id)){
					src_file = my_directory + "/" + src_file;
					System.out.println("Sending chunk " + src_file + " to " + peer.peer_id  );
					/*Path fileLocation = Paths.get(src_file);
					byte[] data = Files.readAllBytes(fileLocation);*/
					File file = new File(src_file);
					FileInputStream fin = new FileInputStream(file);
					ByteArrayOutputStream buffer = new ByteArrayOutputStream();
					byte[] data = new byte[32];
					int nRead;
					while ((nRead = fin.read(data, 0, data.length)) != -1) {
					buffer.write(data, 0, nRead);
					}
					System.out.println("buffer size: "+ buffer.size());
					
					return buffer.toByteArray();
				}else{
					Request rq = new Request(peer, src_file);
					requests.add(rq);

					while(true){
						Thread.sleep(15*1000); 
						Random rand = new Random(); 
						rq = requests.get(rand.nextInt(requests.size()));
						if(peer.peer_id == rq.node.peer_id){
							System.out.println("unchoking peer: " + peer.peer_id);
							src_file = my_directory + "/" + src_file;
							System.out.println("Sending chunk " + src_file + " to " + peer.peer_id  );
							/*Path fileLocation = Paths.get(src_file);
							byte[] data = Files.readAllBytes(fileLocation);*/
							File file = new File(src_file);
							FileInputStream fin = new FileInputStream(file);
							ByteArrayOutputStream buffer = new ByteArrayOutputStream();
							byte[] data = new byte[32];
							int nRead;
							while ((nRead = fin.read(data, 0, data.length)) != -1) {
							buffer.write(data, 0, nRead);
							}
							System.out.println("buffer size: "+ buffer.size());
							
							return buffer.toByteArray();
						}
					}

				}
            }catch (RemoteException e1) {
    			// TODO Auto-generated catch block
    			e1.printStackTrace();
    		}catch (IOException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}catch (InterruptedException e){
				e.printStackTrace();
			}
            
            return null;
    	}
    
   
    
    private static class HeartbeatHandler extends Thread {
        NodeStruct peer;
        int sleep_sec;
        HeartbeatHandler(NodeStruct node, int sec) {
            peer = node;
            sleep_sec = sec;
        }

        public void run() {
            try {
                while (true) {
                                        
                    
                    tracker_stub.heartBeat(peer);
                    System.err.println(peer.peer_id + " sent HeartBeat to tracker " );
                    Thread.sleep(sleep_sec*1000); 
                }
            } catch (RemoteException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    
    public void peerFileRequest(String rar ) {
    	if(rar.compareTo("rarest") == 0) {
    		System.out.println("Using Rarest algorithm to fetch chunks");
        	ArrayList<Pair> sz_chunks = new ArrayList<Pair>(); 
            for( Map.Entry<String, ArrayList<NodeStruct>> entry : req_file_chunk_ids.entrySet() ) {
            	ArrayList<NodeStruct> nodelist = entry.getValue();
            	Pair p = new Pair(nodelist.size(), entry.getKey());
            	sz_chunks.add(p);
            	//
            	
            	
            }
            sz_chunks.sort(new Comparator<Pair>() {
  		    	   @Override
  		    	   public int compare(Pair p1, Pair p2) {
  		    	       return (p1.sz - p2.sz);
  		    	   }
  		    });
            
            for( Pair sc : sz_chunks ) {
            	ArrayList<NodeStruct> nodelist = req_file_chunk_ids.get(sc.chunk);
            	peerFileRecv(nodelist.get(0), sc.chunk, sc.chunk );
            }
    	}else {
    		for( Map.Entry<String, ArrayList<NodeStruct>> entry : req_file_chunk_ids.entrySet() ) {
            	ArrayList<NodeStruct> nodelist = entry.getValue();
            	peerFileRecv(nodelist.get(0), entry.getKey(), entry.getKey() );	
            }
    	}
    }
    
    private static class RecvFileHandler extends Thread {
	    Peer peer;
    	RecvFileHandler(Peer p) {
           peer = p;
        }
    	
    	public void run() {
    		//peer.peerFileRequest(peer.is_rarest);
			System.out.println("Recv File Hndler");
    		while(!peer.req_file_chunk_ids.isEmpty()) {
    			System.out.println("Requesting");
    	    	Iterator<Entry<String, ArrayList<NodeStruct>>> fentryIts = peer.req_file_chunk_ids.entrySet().iterator();
    	    	Iterator<Pair> sziter  = peer.sz_chunks.iterator();
    	    	if(peer.is_rarest.compareTo("rarest") == 0) {
    	    		if(sziter.hasNext()) {
    	    			Pair p = sziter.next();
    	    			ArrayList<NodeStruct> nodelist = peer.req_file_chunk_ids.get(p.chunk);
        				System.out.println("Requesting .. " + p.chunk);
    	    			peer.peerFileRecv(nodelist.get(0), p.chunk, p.chunk );
    	    			sziter.remove();
    	    			peer.req_file_chunk_ids.remove(p.chunk);
    	    		}
    	    		/*if(fentryIts.hasNext()) {
    	    			Entry<String, ArrayList<NodeStruct>> chunk = fentryIts.next();
    	    			
    	    		}*/
    	    		/*for( Pair sc : peer.sz_chunks ) {
    	            	ArrayList<NodeStruct> nodelist = peer.req_file_chunk_ids.get(sc.chunk);
    	            	peer.peerFileRecv(nodelist.get(0), sc.chunk, sc.chunk );
    	            }*/
    	    		
    	    	}
    	    	else {

    	    		if(fentryIts.hasNext()) {
        				Entry<String, ArrayList<NodeStruct>> fe = fentryIts.next();
                    	ArrayList<NodeStruct> nodelist = fe.getValue();
        				System.out.println("Requesting .. " + fe.getKey());
                    	peer.peerFileRecv(nodelist.get(0), fe.getKey(), fe.getKey() );	

        				fentryIts.remove();
        			}
    	    	}
    	    	
    			

    		}
    	}
    }
    
    private static class ReqFileHandler extends Thread {
        NodeStruct n;
        Peer peer;
        int sleep_sec;
        ReqFileHandler(NodeStruct node, int sec,  Peer p) {
            n = node;
            sleep_sec = sec;
            peer = p;
        }


        public void run() {
        	System.out.println("inside reqfilehandler run");
            try {
                while (!peer.req_flag) {
                    System.out.println("getting file locations");     
                	peer.req_flag = peer.fileLocationsRequest();
                	
                	//rarest
                	if(peer.is_rarest.compareTo("rarest") == 0) {
                		System.out.println("Using Rarest algorithm to fetch chunks");
                        for( Map.Entry<String, ArrayList<NodeStruct>> entry : peer.req_file_chunk_ids.entrySet() ) {
                        	ArrayList<NodeStruct> nodelist = entry.getValue();
                        	Pair p = new Pair(nodelist.size(), entry.getKey());
                        	peer.sz_chunks.add(p);
                        	//
                        	
                        	
                        }
                        peer.sz_chunks.sort(new Comparator<Pair>() {
              		    	   @Override
              		    	   public int compare(Pair p1, Pair p2) {
              		    	       return (p1.sz - p2.sz);
              		    	   }
              		    });
                	}
                	
                	System.out.println("hi");
                	if(peer.req_flag) {
                		System.out.println("start file request ");
                		RecvFileHandler rfh = new RecvFileHandler(peer);
                		rfh.start();
                		//peer.peerFileRequest(peer.is_rarest);
                	}
                    System.out.println(n.peer_id + " requested for file " + peer.current_requested_file);
                    Thread.sleep(sleep_sec*1000); //speed of peer
                }
            } catch ( InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    


	private static  class Pair 
	{
	    public int sz;
	    public String chunk;
	   
	    Pair(int s, String c){
	    	sz = s;
	    	chunk = c;
	    }   
	}

	private static  class Request 
	{
	    public NodeStruct node;
	    public String chunk;
	   
	    Request(NodeStruct n, String c){
	    	node = n;
	    	chunk = c;
	    }
	}
	

    public static void main(String[] args) {
    	/**
    	 *order as above
    	 */

        try {
            System.setProperty("java.security.policy","security.policy");

            Peer peer = new Peer(args[0], args[1], Integer.parseInt(args[2]), args[3], args[4], args[5], Integer.parseInt(args[6]), args[7], args[8], Integer.parseInt(args[9]), args[11], args[12], args[13]);
            System.setProperty("java.rmi.server.hostname", peer.my_IP);
            IPeer mystub = (IPeer) UnicastRemoteObject.exportObject(peer, peer.my_port);

            // Bind the remote object's stub in the registry
            //String url = "rmi://" + peer.my_IP + ":" + peer.my_port + "/" + peer.my_id;
            /*String url = "rmi://localhost:1099/"+peer.my_id;
            System.out.println("url: " + url);
            Naming.rebind(url, mystub);*/
            
            Registry registry = LocateRegistry.createRegistry( peer.my_port);
            registry.rebind(peer.my_id,mystub);


            System.out.println(peer.my_id + " ready");
            

            peer.connectDB();
            System.out.println("Connecting to My database " + args[7] );
            //TODO unregister file chunks before start;

            peer.readTable();
            System.out.println("Read Database");
            /*for( Map.Entry<String, ArrayList<String>> fc : peer.file_chunks.entrySet() )
            	System.out.println("filelist: " + fc.getValue().size());
            for(Map.Entry<String, Double> fl: peer.my_file_list.entrySet())
                System.out.println("my_file_list size:" + fl.getKey() + ":" +   fl.getValue());*/

            //registers all the files in the table irrespective of the register flag;
            peer.registerRequestToServer();
            System.out.println("Registered in Tracker");
            peer.fileListRequest();
            System.out.println("Received File List from Tracker");

            HeartbeatHandler handler = new HeartbeatHandler(my_node, Integer.parseInt(args[10]));
            handler.start();

            ReqFileHandler req_handler =  new ReqFileHandler(my_node, Integer.parseInt(args[10]), peer );
            req_handler.start();         
			
        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }
}
