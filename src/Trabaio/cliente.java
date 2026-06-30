package Trabaio;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class cliente {

	public static void main (String[] args) {
		try {
			// Uso: java cliente <ipServidor> <meuIP> <minhaPorta>
			// Exemplo local:  java cliente 127.0.0.1 127.0.0.1 6000
			// Exemplo remoto: java cliente 192.168.1.10 192.168.1.20 6000
			String ipServer = (args.length > 0) ? args[0] : "10.13.79.98";
			String meuIP    = (args.length > 1) ? args[1] : "127.0.0.1";
			int minhaPorta  = (args.length > 2) ? Integer.parseInt(args[2]) : 6000;
			int portaServer = 1099;

			File pastaNode = new File("No_" + minhaPorta);
			if (!pastaNode.exists()) {
				pastaNode.mkdir();
			}

			ServerSocket soqueteNode = new ServerSocket(minhaPorta);
			Registry registry = LocateRegistry.getRegistry(ipServer, portaServer);
			InterfaceRMI coordenadoremoto = (InterfaceRMI) registry.lookup("CoordenadorServidor");
			System.out.println("Conectando ao registro RMI em: " + ipServer);
			coordenadoremoto.NoduloAtivo(meuIP, minhaPorta);
			System.out.println("Conexão efetuada com sucesso! Carregando demais configurações e aceitando envios!");
			while (true) {
				Socket recebimento = soqueteNode.accept();
				DataInputStream dis = new DataInputStream(recebimento.getInputStream());
				int idChunk = dis.readInt();

				if (idChunk == -1) {
					System.out.println("O principal ja enviou todos os pedaços de arquivos.");
					int totalDeChunks = dis.readInt();
					dis.close();
					recebimento.close();

					new Thread(() -> {
						try {
							System.out.println("Verificando posse de chunks...");
							List<Integer> meusChunks = coordenadoremoto.buscarChunkDono(meuIP + ":" + minhaPorta);
							System.out.println("Possuo "+meusChunks.size()+" chunks: "+meusChunks);

							Scanner scanner = new Scanner(System.in);
							System.out.println("//////// AGORA, DIGITE 'SIM' PARA COMEÇAR A REQUISITAR O ARQUIVO INTEIRO");
							String input = scanner.nextLine();
							if (input.equalsIgnoreCase("sim")) {
								for (int i = 0; i < totalDeChunks; i++) {
									if (meusChunks.contains(i)) {
										continue;
									}
									String dono = coordenadoremoto.buscarDonoChunk(i);
									String[] partes = dono.split(":");
									String donoIp = partes[0];
									int donoPorta = Integer.parseInt(partes[1]);
									Socket envioReq = new Socket(donoIp, donoPorta);
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
								}
								String nomeArqv = coordenadoremoto.getNomeArquivo();
								File arquivoTotal = new File(pastaNode, "Reconstruido_"+nomeArqv);
								try (FileOutputStream funil = new FileOutputStream(arquivoTotal, true)) {
									for (int j = 0; j < totalDeChunks; j++) {
										File chunkLocal = new File(pastaNode, "chunk_"+j+".part");
										try (FileInputStream canudo = new FileInputStream(chunkLocal)) {
											byte[] buffer = canudo.readAllBytes();
											funil.write(buffer);
											System.out.println("Parte "+j+" do arquivo anexada!");
										} catch (IOException e) {
											e.printStackTrace();
										}
									}
									System.out.println("Arquivo reconstruído para este nó!");
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}).start();

					continue;

				} else if (idChunk == -2) {
					int chunkReq = dis.readInt();
					System.out.println("Um dos nós requisitou o chunk "+chunkReq);
					File chunkLocal = new File(pastaNode, "chunk_"+chunkReq+".part");
					if (chunkLocal.exists()) {
						try (FileInputStream canudo = new FileInputStream(chunkLocal)) {
							byte[] bufferChunk = canudo.readAllBytes();
							DataOutputStream envio = new DataOutputStream(recebimento.getOutputStream());
							envio.writeInt((int) chunkLocal.length());
							envio.write(bufferChunk);
							envio.close();
							recebimento.close();
							System.out.println("Envio feito com sucesso!");
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					continue;

				} else if (idChunk == -3) {
					DataOutputStream ack = new DataOutputStream(recebimento.getOutputStream());
					ack.writeInt(1);
					ack.flush();
					recebimento.close();
					continue;

				} else {
					int tamArqv = dis.readInt();
					byte[] buffer = new byte[tamArqv];
					dis.readFully(buffer);
					File arquivoMoldado = new File(pastaNode, "chunk_"+idChunk+".part");
					FileOutputStream funil = new FileOutputStream(arquivoMoldado);
					funil.write(buffer);
					coordenadoremoto.registrarPosseChunk(idChunk, meuIP + ":" + minhaPorta);
					System.out.println("Guardei o pedaço "+idChunk+" do arquivo com sucesso!");
					funil.close();
					recebimento.close();
				}
			}

		} catch (Exception e) {
			System.err.println("Erro no Nó Cliente: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
