package Trabaio;
import java.rmi.server.UnicastRemoteObject;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Coordenad extends UnicastRemoteObject implements InterfaceRMI {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	private Map <Integer, String> tabelaChunks;
	private ArrayList<String> NosAtivos;
	private ArrayList<Integer> PortaNosAtivos;
	private Socket soquete;
	
	public Coordenad() throws IOException {
	
		super();
		this.tabelaChunks = new HashMap<>();
		this.NosAtivos = new ArrayList<>();
		this.PortaNosAtivos = new ArrayList<>();
		//this.soquete = new Socket();
	}
	@Override
	public String buscarDonoChunk(int chunkId) throws RemoteException{
		
		return this.tabelaChunks.get(chunkId);
	}
	@Override
	
	public void registrarPosseChunk (int chunkId, String IpDoNode) throws RemoteException{
		this.tabelaChunks.put(chunkId, IpDoNode);
		
	}
	@Override
	public List<Integer> buscarChunkDono(String Ip){
		List<Integer> chunksDoNode = new ArrayList<>();
		for (Map.Entry<Integer, String> entrada : this.tabelaChunks.entrySet()) {
			if (entrada.getValue().equals(Ip)) {
				chunksDoNode.add(entrada.getKey());
			}
		}
		return chunksDoNode; 
	}
	@Override
	public int verificaVazio() throws RemoteException {
		return this.tabelaChunks.size();
	}
	@Override
	public void NoduloAtivo(String Ip, int Porta) throws RemoteException {
		this.NosAtivos.add(Ip);
		this.PortaNosAtivos.add(Porta);
		System.out.println("Conexão com o nó de Ip " + Ip + "De porta " + Porta + ". Mas ainda aguardando conexões até segunda ordem.");
		
	}
	@Override
	public ArrayList<String> getNosAtivos() throws RemoteException{
		return this.NosAtivos;
	}
	@Override
	public ArrayList<Integer> getPortaNosAtivos() throws RemoteException{
		return this.PortaNosAtivos;
	}
	
	// esse metodo abaixo nao deveria estar aqui, e sim no ServidorPrincipal pq aqui é so um no coordenador que vai mapear os nós na rede.
	public void enviarPartes(byte[] buffer, String noAtual, int Porta, int bytesLidos, int idChunk) throws UnknownHostException, IOException {
		this.soquete = new Socket(noAtual, Porta);
		// parte de envio de dados agora, com DataOutputStream e DataInputStream
		DataOutputStream dos = new DataOutputStream(this.soquete.getOutputStream());
		// dados sao enviados NESTA ORDEM.
		dos.writeInt(idChunk); // envio do ID do chunk
		dos.writeInt(bytesLidos); // envio de quantos bytes foram lidos
		dos.write(buffer, 0, bytesLidos);
		this.soquete.close();
		// lembrando: DataOutputStream é para dados que vao ser enviados (associar com saída)
		// DataInputStream é para dados que estão sendo recebidos
		// FileOutputStream e FileInputStream funcionam ao contrario, o primeiro é para dados recebidos que vao compor um arquivo
		// e o segundo é para dados que vao ser enviados de um arquivo especifico
	}
	
	

}
