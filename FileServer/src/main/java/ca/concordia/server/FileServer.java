package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class FileServer {

    private final FileSystemManager fsManager;
    private final int port;

    // Constructor matching Main.java expectations
    public FileServer(int port, String fileSystemName, int totalSize) {
        // Initialize the FileSystemManager with fixed size (10 blocks × 128 bytes)
        FileSystemManager fsManager = new FileSystemManager(fileSystemName, 10 * 128);
        this.fsManager = fsManager;
        this.port = port;
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server started. Listening on port " + port + "...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Accepted connection from: " + clientSocket);

                Thread clientThread = new Thread(() -> handleClient(clientSocket));
                clientThread.start();
            }
        } catch (Exception e) {
            System.err.println("Server error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleClient(Socket clientSocket) {
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true, StandardCharsets.UTF_8)
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    writer.println("ERROR: Empty command.");
                    continue;
                }

                System.out.println("Received: " + line);
                String[] parts = line.split(" ", 3);
                String command = parts[0].toUpperCase();

                try {
                    switch (command) {
                        case "CREATE":
                            if (parts.length < 2) {
                                writer.println("ERROR: Missing filename.");
                                break;
                            }
                            fsManager.createFile(parts[1]);
                            writer.println("SUCCESS: File '" + parts[1] + "' created.");
                            break;

                        case "WRITE":
                            if (parts.length < 3) {
                                writer.println("ERROR: Missing filename or content.");
                                break;
                            }
                            fsManager.writeFile(parts[1], parts[2].getBytes(StandardCharsets.UTF_8));
                            writer.println("SUCCESS: File '" + parts[1] + "' written.");
                            break;

                        case "READ":
                            if (parts.length < 2) {
                                writer.println("ERROR: Missing filename.");
                                break;
                            }
                            byte[] data = fsManager.readFile(parts[1]);
                            writer.println("SUCCESS: " + new String(data, StandardCharsets.UTF_8));
                            break;

                        case "DELETE":
                            if (parts.length < 2) {
                                writer.println("ERROR: Missing filename.");
                                break;
                            }
                            fsManager.deleteFile(parts[1]);
                            writer.println("SUCCESS: File '" + parts[1] + "' deleted.");
                            break;

                        case "LIST":
                            String[] files = fsManager.listFiles();
                            writer.println("FILES: " + String.join(",", files));
                            break;

                        case "QUIT":
                            writer.println("SUCCESS: Disconnecting.");
                            return;

                        default:
                            writer.println("ERROR: Unknown command.");
                    }
                } catch (Exception e) {
                    writer.println("ERROR: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("Client error: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
                System.out.println("Connection closed: " + clientSocket);
            } catch (Exception ignored) {}
        }
    }
}