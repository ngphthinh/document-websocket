import { Client } from "@stomp/stompjs";
import SockJS from "sockjs-client";

class WebSocketService {
  private client: Client;
  private connectedPromise: Promise<void>;

  constructor() {
    let resolveConnection!: () => void;

    this.connectedPromise = new Promise((resolve) => {
      resolveConnection = resolve;
    });

    this.client = new Client({
      webSocketFactory: () => new SockJS("http://localhost:8080/ws"),
      reconnectDelay: 5000,

      onConnect: () => {
        console.log("WebSocket Connected");
        resolveConnection();
      },
    });
  }

  connect() {
    if (!this.client.active) {
      this.client.activate();
    }

    return this.connectedPromise;
  }

  async subscribe(
    destination: string,
    callback: (message: any) => void
  ) {
    await this.connect();

    return this.client.subscribe(destination, callback);
  }

  async publish(destination: string, body?: unknown) {
    await this.connect();

    this.client.publish({
      destination,
      body: body ? JSON.stringify(body) : "",
    });
  }
}

export const websocketService = new WebSocketService();