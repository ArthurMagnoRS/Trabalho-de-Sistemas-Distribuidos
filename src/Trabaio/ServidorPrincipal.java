package Trabaio;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class ServidorPrincipal {

	private static volatile boolean heartbeatStartado = false;
	private static volatile boolean servidorChunksIniciado = false;
	private static final int PORTA_SERVIDOR_CHUNKS = 7000;
	private static final int CHUNK_SIZE = 1024 * 1024;

	public static void main(String[] args) {
		try {
			Registry registry;
			try {
				registry = LocateRegistry.createRegistry(1099);
				System.out.println("Novo RMI Registry criado na porta 1099.");
			} catch (ExportException e) {
				registry = LocateRegistry.getRegistry(1099);
				System.out.println("RMI Registry já estava rodando. Aproveitando o existente.");
			}
			Coordenad cordenador = new Coordenad();
			registry.rebind("CoordenadorServidor", cordenador);
			System.out.println("Servidor RMI está online e aguardando conexões.");
			Scanner scanner = new Scanner(System.in);
			while (true) {
				System.out.println("---------//MENU//------------");
				System.out.println("Digite o caminho para o arquivo que quer compartilhar");
				System.out.println("Digite 'sair' para encerrar o servidor.");

				String input = scanner.nextLine();

				if (input.equalsIgnoreCase("sair")) {
					System.out.println("Desligando servidor.");
					System.exit(0);
				} else {
					System.out.println("Preparando para compartilhar o arquivo " + input);
					System.out.println("Existem " + cordenador.getNosAtivos().size() + " nós disponíveis para compartilhar.");

					File arquivo = new File(input);
					cordenador.setNomeArquivo(arquivo.getName());
					try (FileInputStream funil = new FileInputStream(arquivo)) {
						byte[] bufferArqv = new byte[CHUNK_SIZE];
						System.out.println("Tamanho do arquivo em bytes: " + arquivo.length());
						int bytesLidos = 0;
						int idChunk = 0;
						ArrayList<String> nosAtivos = cordenador.getNosAtivos();
						System.out.println(nosAtivos);
						ArrayList<Integer> portaRespec = cordenador.getPortaNosAtivos();
						System.out.println(portaRespec);
						int numNosAtivos = nosAtivos.size();
						while ((bytesLidos = funil.read(bufferArqv)) != -1) {
							int noAtual = idChunk % numNosAtivos;
							enviarPartes(bufferArqv, nosAtivos.get(noAtual), portaRespec.get(noAtual), bytesLidos, idChunk);
							idChunk++;
						}
						System.out.println("Arquivo inteiro dividido e compartilhado com sucesso!");
						byte[] bufferAviso = new byte[0];
						for (int i = 0; i < nosAtivos.size(); i++) {
							enviarPartes(bufferAviso, nosAtivos.get(i), portaRespec.get(i), idChunk, -1);
						}
						cordenador.setCaminhoArquivo(input);
						cordenador.setTotalChunks(idChunk);
						iniciarHeartbeat(cordenador);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void iniciarHeartbeat(Coordenad cordenador) {
		if (heartbeatStartado) return;
		heartbeatStartado = true;
		Thread hb = new Thread(() -> {
			while (true) {
				try {
					Thread.sleep(5000);
					ArrayList<String> nos = cordenador.getNosAtivos();
					ArrayList<Integer> portas = cordenador.getPortaNosAtivos();
					for (int i = 0; i < nos.size(); i++) {
						String ip = nos.get(i);
						int porta = portas.get(i);
						if (!pingNode(ip, porta)) {
							String nodeId = ip + ":" + porta;
							System.out.println("\nNó " + nodeId + " desconectado!");
							List<Integer> chunksLostos = cordenador.buscarChunkDono(nodeId);
							cordenador.removerNoAtivo(ip, porta);
							ArrayList<String> nosRestantes = cordenador.getNosAtivos();
							ArrayList<Integer> portasRestantes = cordenador.getPortaNosAtivos();
							if (nosRestantes.isEmpty()) {
								System.out.println("Nenhum nó restante. Chunks perdidos.");
							} else if (nosRestantes.size() == 1) {
								System.out.println("Apenas 1 nó restante. Servidor assumirá os "
										+ chunksLostos.size() + " chunks perdidos para download direto.");
								iniciarServidorDeChunks(cordenador, chunksLostos);
							} else {
								System.out.println("Redistribuindo " + chunksLostos.size()
										+ " chunks entre " + nosRestantes.size() + " nós restantes...");
								redistribuirChunks(cordenador, chunksLostos, nosRestantes, portasRestantes);
							}
							break;
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		hb.setDaemon(true);
		hb.start();
	}

	private static boolean pingNode(String ip, int porta) {
		try (Socket s = new Socket()) {
			s.connect(new InetSocketAddress(ip, porta), 2000);
			s.setSoTimeout(3000);
			DataOutputStream dos = new DataOutputStream(s.getOutputStream());
			dos.writeInt(-3);
			dos.flush();
			DataInputStream dis = new DataInputStream(s.getInputStream());
			dis.readInt();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private static void redistribuirChunks(Coordenad cordenador, List<Integer> chunksLostos,
			ArrayList<String> nos, ArrayList<Integer> portas) {
		String caminhoArquivo;
		try {
			caminhoArquivo = cordenador.getCaminhoArquivo();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		int idx = 0;
		for (int chunkId : chunksLostos) {
			try {
				long offset = (long) chunkId * CHUNK_SIZE;
				byte[] buffer = new byte[CHUNK_SIZE];
				int bytesLidos;
				try (FileInputStream fis = new FileInputStream(caminhoArquivo)) {
					fis.skipNBytes(offset);
					bytesLidos = fis.read(buffer);
				}
				if (bytesLidos > 0) {
					int noAlvo = idx % nos.size();
					enviarPartes(buffer, nos.get(noAlvo), portas.get(noAlvo), bytesLidos, chunkId);
					cordenador.registrarPosseChunk(chunkId, nos.get(noAlvo) + ":" + portas.get(noAlvo));
					System.out.println("Chunk " + chunkId + " redistribuído para "
							+ nos.get(noAlvo) + ":" + portas.get(noAlvo));
					idx++;
				}
			} catch (Exception e) {
				System.err.println("Erro ao redistribuir chunk " + chunkId + ": " + e.getMessage());
			}
		}
		System.out.println("Redistribuição concluída.");
	}

	private static void iniciarServidorDeChunks(Coordenad cordenador, List<Integer> chunksLostos) {
		String caminhoArquivo;
		try {
			caminhoArquivo = cordenador.getCaminhoArquivo();
			String servidorNodeId = "127.0.0.1:" + PORTA_SERVIDOR_CHUNKS;
			for (int chunkId : chunksLostos) {
				cordenador.registrarPosseChunk(chunkId, servidorNodeId);
			}
			System.out.println("Servidor registrado como fonte de " + chunksLostos.size()
					+ " chunks na porta " + PORTA_SERVIDOR_CHUNKS
					+ ". O nó restante pode solicitar o download completo digitando 'SIM'.");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		if (servidorChunksIniciado) return;
		servidorChunksIniciado = true;
		Thread t = new Thread(() -> {
			try (ServerSocket ss = new ServerSocket(PORTA_SERVIDOR_CHUNKS)) {
				while (true) {
					Socket clienteSocket = ss.accept();
					new Thread(() -> {
						try {
							DataInputStream dis = new DataInputStream(clienteSocket.getInputStream());
							int flag = dis.readInt();
							if (flag == -2) {
								int chunkId = dis.readInt();
								long offset = (long) chunkId * CHUNK_SIZE;
								byte[] buffer = new byte[CHUNK_SIZE];
								int bytesLidos;
								try (FileInputStream fis = new FileInputStream(caminhoArquivo)) {
									fis.skipNBytes(offset);
									bytesLidos = fis.read(buffer);
								}
								DataOutputStream dos = new DataOutputStream(clienteSocket.getOutputStream());
								dos.writeInt(bytesLidos);
								dos.write(buffer, 0, bytesLidos);
								dos.flush();
							}
							clienteSocket.close();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}).start();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		t.setDaemon(true);
		t.start();
	}

	private static void enviarPartes(byte[] buffer, String noAtual, int Porta, int bytesLidos, int idChunk) throws UnknownHostException, IOException {
		try (Socket soquete = new Socket(noAtual, Porta)) {
			DataOutputStream dos = new DataOutputStream(soquete.getOutputStream());
			dos.writeInt(idChunk);
			dos.writeInt(bytesLidos);
			if (idChunk != -1) {
				dos.write(buffer, 0, bytesLidos);
			}
		}
	}
}
