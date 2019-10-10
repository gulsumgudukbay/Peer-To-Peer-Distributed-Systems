import java.io.Serializable;

//@SuppressWarnings("serial")
public class QueueStruct implements Serializable {
	
	NodeStruct peer;
	String chunk_name;
	public QueueStruct(NodeStruct p, String c ){
		peer = p;
		chunk_name = c;
		
	}
}
