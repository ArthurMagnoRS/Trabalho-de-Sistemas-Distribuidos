package Trabaio;
import java.rmi.server.UnicastRemoteObject;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Coordenad extends UnicastRemoteObject implements InterfaceRMI {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	private Map<Integer, String> tabelaChunks;
	private List<String> NosAtivos;
	private List<Integer> PortaNosAtivos;
	private String nome;
	private String caminhoArquivo;
	private int totalChunks;
	
	public Coordenad() throws IOException {

		super();
		this.tabelaChunks = new ConcurrentHashMap<>();
		this.NosAtivos = new CopyOnWriteArrayList<>();
		this.PortaNosAtivos = new CopyOnWriteArrayList<>();
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
	public List<Integer> buscarChunkDono(String Ip) throws RemoteException{
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
	public synchronized void NoduloAtivo(String Ip, int Porta) throws RemoteException {
		this.NosAtivos.add(Ip);
		this.PortaNosAtivos.add(Porta);
		System.out.println("Conexão com o nó de Ip " + Ip + "De porta " + Porta + ". Mas ainda aguardando conexões até segunda ordem.");
		
	}
	@Override
	public ArrayList<String> getNosAtivos() throws RemoteException{
		return new ArrayList<>(this.NosAtivos);
	}
	@Override
	public ArrayList<Integer> getPortaNosAtivos() throws RemoteException{
		return new ArrayList<>(this.PortaNosAtivos);
	}
	
	@Override
	public void setNomeArquivo(String nome) throws RemoteException {
		this.nome = nome;
	}

	@Override
	public String getNomeArquivo() throws RemoteException {
		return this.nome;
	}

	@Override
	public synchronized void removerNoAtivo(String ip, int porta) throws RemoteException {
		for (int i = 0; i < NosAtivos.size(); i++) {
			if (NosAtivos.get(i).equals(ip) && PortaNosAtivos.get(i).equals(porta)) {
				NosAtivos.remove(i);
				PortaNosAtivos.remove(i);
				break;
			}
		}
	}

	@Override
	public void setCaminhoArquivo(String caminho) throws RemoteException {
		this.caminhoArquivo = caminho;
	}

	@Override
	public String getCaminhoArquivo() throws RemoteException {
		return this.caminhoArquivo;
	}

	@Override
	public void setTotalChunks(int total) throws RemoteException {
		this.totalChunks = total;
	}

	@Override
	public int getTotalChunks() throws RemoteException {
		return this.totalChunks;
	}
}
