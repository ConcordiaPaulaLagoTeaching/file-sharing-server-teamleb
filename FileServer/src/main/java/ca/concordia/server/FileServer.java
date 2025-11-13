package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileServer {

    private final FileSystemManager fs;
    private final int port;

    private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

    public FileServer(int port, String fsName,
                      int blockSize, int maxFiles, int maxBlocks, int totalSizeBytes) {
        this.port = port;
        this.fs = new FileSystemManager(fsName, blockSize, maxFiles, maxBlocks, totalSizeBytes);
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server started. Listening on port " + port + "...");
            while (true) {
                Socket client = serverSocket.accept();
                System.out.println("Handling client: " + client);
                pool.submit(() -> handle(client));
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Could not start server on port " + port);
        }
    }

    private void handle(Socket client) {
        try (BufferedReader reader = new BufferedReader(
                 new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter writer = new PrintWriter(
                 new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8), true)) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                String[] parts = line.split(" ", 3);
                String command = parts[0].toUpperCase();

                try {
                    switch (command) {
                        case "CREATE": {
                            if (parts.length < 2) { writer.println("ERROR: missing filename"); break; }
                            fs.createFile(parts[1]);
                            writer.println("OK");
                            break;
                        }
                        case "WRITE": {
                            if (parts.length < 3) { writer.println("ERROR: missing filename or content"); break; }
                            String filename = parts[1];
                            String content = parts[2];
                            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
                            fs.writeFile(filename, bytes);
                            writer.println("OK " + bytes.length);
                            break;
                        }
                        case "READ": {
                            if (parts.length < 2) { writer.println("ERROR: missing filename"); break; }
                            byte[] data = fs.readFile(parts[1]);
                            String payload = new String(data, StandardCharsets.UTF_8);
                            System.out.println("DEBUG READ -> size=" + data.length + " payload='" + payload + "'");
                            writer.println("OK " + data.length + (data.length > 0 ? " " + payload : ""));
                            break;
                        }
                        case "DELETE": {
                            if (parts.length < 2) { writer.println("ERROR: missing filename"); break; }
                            fs.deleteFile(parts[1]);
                            writer.println("OK");
                            break;
                        }
                        case "LIST": {
                            String[] names = fs.listFiles();
                            writer.println("OK " + names.length + (names.length > 0 ? " " + String.join(" ", Arrays.asList(names)) : ""));
                            break;
                        }
                        case "QUIT":
                        case "EXIT":
                            writer.println("OK bye");
                            return;
                        default:
                            writer.println("ERROR: Unknown command");
                    }
                } catch (Exception e) {
                    String msg = e.getMessage();
                    writer.println((msg != null && msg.startsWith("ERROR:")) ? msg : "ERROR: " + msg);
                }
            }
        } catch (IOException ignored) {
        } finally {
            try { client.close(); } catch (IOException ignored) {}
        }
    }
}