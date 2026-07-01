package Trabaio;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class cliente {

	// ====================================================================
	// GLOBAIS PARA ACESSO FÁCIL
	// ====================================================================
	private static ConcurrentHashMap<Integer, Set<String>> cacheChunks = new ConcurrentHashMap<>();
	private static Set<String> knownPeers = ConcurrentHashMap.newKeySet();

	public static void main (String[] args) {
		try {
			String ipServer = "127.0.0.1";
			String meuIP = "127.0.0.1";
			int minhaPorta = 5000;
			int portaServer = 1099;
			
			String meuNodeId = meuIP + ":" + minhaPorta;
			
			File pastaNode = new File("No_" + minhaPorta);
			if (!pastaNode.exists()) {
				pastaNode.mkdir(); 
			}
			
			ServerSocket soqueteNode = new ServerSocket(minhaPorta);
			Registry registry = LocateRegistry.getRegistry(ipServer, portaServer);
			InterfaceRMI coordenadoremoto = (InterfaceRMI) registry.lookup("CoordenadorServidor");
			
			System.out.println("Conectando ao registro RMI em: " + ipServer);
			coordenadoremoto.NoduloAtivo(meuIP, minhaPorta);
			
			// ========================================================
			// BOOTSTRAP DOS AMIGOS INICIAIS
			// ========================================================
			ArrayList<String> ipsAtivos = coordenadoremoto.getNosAtivos();
			ArrayList<Integer> portasAtivas = coordenadoremoto.getPortaNosAtivos();
			for (int i = 0; i < ipsAtivos.size(); i++) {
				String peerExistente = ipsAtivos.get(i) + ":" + portasAtivas.get(i);
				if (!peerExistente.equals(meuNodeId)) {
					knownPeers.add(peerExistente); 
				}
			}
			
			// ========================================================
			// THREAD DE FOFOCA (GOSSIP RANDOMIZADO)
			// ========================================================
			new Thread(() -> {
				Random random = new Random();
				while(true) {
					try {
						Thread.sleep(3000); 
						if (knownPeers.isEmpty()) continue;
						
						// 1. Converte o Set para Lista para podermos sortear um colega aleatório!
						List<String> listaPeers = new ArrayList<>(knownPeers);
						listaPeers.remove(meuNodeId); // Prevenção extra
						
						if (!listaPeers.isEmpty()) {
							// 2. Sorteia um índice aleatório
							int randomIndex = random.nextInt(listaPeers.size());
							String peerAlvo = listaPeers.get(randomIndex);
							
							String[] partes = peerAlvo.split(":");
							try (Socket s = new Socket(partes[0], Integer.parseInt(partes[1]))) {
								DataOutputStream dos = new DataOutputStream(s.getOutputStream());
								dos.writeInt(-5); // FLAG GOSSIP
								
								// 3. O Fofoqueiro apresenta-se primeiro! (Resolve o "Nó Fantasma")
								dos.writeUTF(meuNodeId);
								
								dos.writeInt(cacheChunks.size()); 
								for (Map.Entry<Integer, Set<String>> entrada : cacheChunks.entrySet()) {
									dos.writeInt(entrada.getKey()); 
									dos.writeInt(entrada.getValue().size()); 
									for (String donoIP : entrada.getValue()) {
										dos.writeUTF(donoIP); 
									}
								}
								dos.flush();
								System.out.println("[GOSSIP] Enviei a minha fofoca para o colega aleatório: " + peerAlvo);
							} catch (Exception e) {
								knownPeers.remove(peerAlvo); // O colega caiu
							}
						}
					} catch (Exception e) { }
				}
			}).start();
			
			// ========================================================
			
			List<Integer> myChunks = coordenadoremoto.buscarChunkDono(meuNodeId); 
			if (!myChunks.isEmpty()) {
				System.out.println("O RMI indica que ja possuo chunks: " + myChunks);
				for (int c : myChunks) {
					cacheChunks.computeIfAbsent(c, k -> ConcurrentHashMap.newKeySet()).add(meuNodeId);
				}
				new Thread(() -> iniciarDownloadP2P(coordenadoremoto, meuNodeId, pastaNode)).start();
			}
				
			System.out.println("Conexão efetuada com sucesso! À escuta...");
			
			while (true) {
				Socket recebimento = soqueteNode.accept();
				DataInputStream dis = new DataInputStream(recebimento.getInputStream());
				int idChunk = dis.readInt(); 
				
				if (idChunk == -1) {
					int totalDeChunks = dis.readInt();
					System.out.println("O principal ja enviou todos os pedaços (" + totalDeChunks + " no total).");
					dis.close();
					recebimento.close();
					
					new Thread(() -> iniciarDownloadP2P(coordenadoremoto, meuNodeId, pastaNode)).start();
					continue;
					
				} else if (idChunk == -2) { 
					int chunkReq = dis.readInt();
					System.out.println("Um colega requisitou o chunk " + chunkReq);
					File chunkLocal = new File(pastaNode, "chunk_"+chunkReq+".part");
					if (chunkLocal.exists()) {
						try(FileInputStream canudo = new FileInputStream(chunkLocal)){
							byte[] bufferChunk = canudo.readAllBytes();
							DataOutputStream envio = new DataOutputStream(recebimento.getOutputStream());
							envio.writeInt((int) chunkLocal.length()); 
							envio.write(bufferChunk); 
							canudo.close();
							envio.close();
							recebimento.close();
							System.out.println("Envio feito com sucesso!");
						} catch (IOException e) { e.printStackTrace(); }
					}
					continue;
					
				} else if (idChunk == -3) { 
					DataOutputStream ack = new DataOutputStream(recebimento.getOutputStream());
					ack.writeInt(1);
					ack.flush();
					recebimento.close();
					continue;
					
				} else if (idChunk == -5) { // SINAL DE FOFOCA RECEBIDA
					
					// 1. Lê quem é o autor da fofoca e guarda na lista de amigos!
					String autorFofoca = dis.readUTF();
					if (!autorFofoca.equals(meuNodeId)) {
						knownPeers.add(autorFofoca);
					}
					
					int numEntradas = dis.readInt();
					int infosAprendidas = 0;
					
					for (int i = 0; i < numEntradas; i++) {
						int cId = dis.readInt();
						int donosCount = dis.readInt();
						for (int d = 0; d < donosCount; d++) {
							String donoIP = dis.readUTF(); 
							
							boolean isNovo = cacheChunks.computeIfAbsent(cId, k -> ConcurrentHashMap.newKeySet()).add(donoIP);
							if (isNovo && !donoIP.equals(meuNodeId)) {
								infosAprendidas++;
								knownPeers.add(donoIP); 
							}
						}
					}
					
					if (infosAprendidas > 0) {
						System.out.println("[GOSSIP RECEBIDO] O colega " + autorFofoca + " fofocou! Aprendi " + infosAprendidas + " novas localizações de chunks.");
					} else {
						System.out.println("[GOSSIP RECEBIDO] Fofoca do colega " + autorFofoca + " processada. Sem novidades.");
					}
					recebimento.close();
					continue;
					
				} else { // RECEBEU DO SERVIDOR RMI
					int tamArqv = dis.readInt(); 
					byte[] buffer = new byte[tamArqv]; 
					dis.readFully(buffer);
					
					File arquivoMoldado = new File(pastaNode, "chunk_"+idChunk+".part");
					FileOutputStream funil = new FileOutputStream(arquivoMoldado);
					funil.write(buffer);
					
					coordenadoremoto.registrarPosseChunk(idChunk, meuNodeId);
					cacheChunks.computeIfAbsent(idChunk, k -> ConcurrentHashMap.newKeySet()).add(meuNodeId);
					
					System.out.println("Guardei o pedaço " + idChunk + " do arquivo com sucesso!");
					funil.close();
					recebimento.close();
				}
			}
		} catch (Exception e) {
            System.err.println("Erro no Nó Cliente: " + e.getMessage());
            e.printStackTrace();
		}
	}
	
	// ========================================================
	// LÓGICA P2P COM CACHE E PROTEÇÃO DE QUEDAS DO RMI
	// ========================================================
	private static void iniciarDownloadP2P(InterfaceRMI coordenador, String meuNodeId, File pastaNode) {
		try {
			Scanner scanner = new Scanner(System.in);
			System.out.println("\n//////// DIGITE 'SIM' PARA COMEÇAR A REQUISITAR O ARQUIVO INTEIRO");
			if (!scanner.nextLine().equalsIgnoreCase("sim")) return;
			
			int totalDeChunks = 0;
			try {
				totalDeChunks = coordenador.getTotalChunks();
			} catch (Exception e) {
				System.out.println("[AVISO] Servidor Central RMI offline! Tentando adivinhar o total pelo Cache...");
				for (Integer key : cacheChunks.keySet()) {
					if (key >= totalDeChunks) totalDeChunks = key + 1;
				}
			}
			
			List<Integer> meusChunks = new ArrayList<>();
			for (Map.Entry<Integer, Set<String>> entry : cacheChunks.entrySet()) {
				if (entry.getValue().contains(meuNodeId)) meusChunks.add(entry.getKey());
			}
			
			for (int i = 0; i < totalDeChunks; i++) {
				if (meusChunks.contains(i)) continue;
				
				String dono = null;
				
				Set<String> donosNoCache = cacheChunks.get(i);
				if (donosNoCache != null && !donosNoCache.isEmpty()) {
					for (String d : donosNoCache) {
						if (!d.equals(meuNodeId)) {
							dono = d;
							break;
						}
					}
					if (dono != null) System.out.println("[CACHE] Pelo meu cache, sei que " + dono + " tem o chunk " + i);
				}
				
				if (dono == null) {
					try {
						dono = coordenador.buscarDonoChunk(i); 
						if (dono != null) {
							System.out.println("[TRACKER] O RMI indicou que " + dono + " tem o chunk " + i);
							cacheChunks.computeIfAbsent(i, k -> ConcurrentHashMap.newKeySet()).add(dono);
							knownPeers.add(dono);
						}
					} catch (Exception e) {
						System.out.println("[AVISO] RMI Offline! Não foi possível perguntar ao coordenador.");
					}
				}
				
				if (dono == null) {
					System.out.println("ERRO FATAL: Ninguém possui o chunk " + i + ".");
					return; 
				}
				
				String[] partes = dono.split(":");
				Socket envioReq = new Socket(partes[0], Integer.parseInt(partes[1]));
				DataOutputStream dos = new DataOutputStream(envioReq.getOutputStream());
				DataInputStream disResposta = new DataInputStream(envioReq.getInputStream());
				
				dos.writeInt(-2); 
				dos.writeInt(i); 
				
				int tamResp = disResposta.readInt();
				byte[] buffer = new byte[tamResp];
				disResposta.readFully(buffer); 
				
				File arqvMolde = new File(pastaNode, "chunk_"+i+".part");
				FileOutputStream funil = new FileOutputStream(arqvMolde);
				funil.write(buffer);
				funil.close();
				envioReq.close();
				
				try {
					coordenador.registrarPosseChunk(i, meuNodeId);
				} catch (Exception e) {} 
				
				cacheChunks.computeIfAbsent(i, k -> ConcurrentHashMap.newKeySet()).add(meuNodeId);
				meusChunks.add(i);
				System.out.println("Chunk " + i + " baixado do colega " + dono);
			}
			
			String nomeArqv = "video_reconstruido.mp4"; 
			try { nomeArqv = coordenador.getNomeArquivo(); } catch (Exception e) {}
			
			File arquivoTotal = new File(pastaNode, "Reconstruido_" + nomeArqv);
			if(arquivoTotal.exists()) arquivoTotal.delete();
			
			try (FileOutputStream funil = new FileOutputStream(arquivoTotal, true)){
				for (int j = 0; j < totalDeChunks; j++) {
					File chunkLocal = new File (pastaNode,"chunk_"+j+".part");
					try (FileInputStream canudo = new FileInputStream(chunkLocal)){
						byte[] buffer = canudo.readAllBytes();
						funil.write(buffer);
					}
				}
				System.out.println("\n*** ARQUIVO RECONSTRUÍDO COM SUCESSO! ***\n");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}