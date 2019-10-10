import java.io.Serializable;

@SuppressWarnings("serial")
public class NodeStruct implements Serializable {
	String peer_id, peer_ip;
	int peer_port;
	public NodeStruct(String id, String ip, int port ){
		peer_id = id;
		peer_ip = ip;
		peer_port = port;
	}
}
