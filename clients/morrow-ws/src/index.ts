export type ConnectionState = "idle" | "connecting" | "connected" | "disconnected" | "reconnecting";

export interface MorrowAuth {
  clientId: string;
  signNonce: (nonce: string) => Promise<string>;
}

export interface MorrowOptions {
  url: string;
  durableId?: string;
  auth?: MorrowAuth;
  reconnect?: boolean;
  reconnectMinDelayMs?: number;
  reconnectMaxDelayMs?: number;
  maxQueuedMessages?: number;
}

export interface MorrowMessage {
  subject: string;
  sid: string;
  replyTo?: string;
  ackSubject?: string;
  headers?: Record<string, string>;
  payload: Uint8Array;
}

export class MorrowWebSocketClient extends EventTarget {
  private readonly options: Required<Pick<MorrowOptions, "reconnectMinDelayMs" | "reconnectMaxDelayMs" | "maxQueuedMessages">> & MorrowOptions;
  private socket: WebSocket | undefined;
  private stateValue: ConnectionState = "idle";
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof window.setTimeout> | undefined;
  private receiveBuffer = new Uint8Array();
  private readonly messages: MorrowMessage[] = [];
  private readonly subscriptions = new Map<string, { subject: string; queue?: string }>();
  private readonly waiters: Array<{
    predicate: (message: MorrowMessage) => boolean;
    resolve: (message: MorrowMessage) => void;
    reject: (error: Error) => void;
    timer: ReturnType<typeof window.setTimeout>;
  }> = [];
  private connectResolve: (() => void) | undefined;
  private connectReject: ((error: Error) => void) | undefined;

  constructor(options: MorrowOptions) {
    super();
    this.options = {
      reconnectMinDelayMs: 250,
      reconnectMaxDelayMs: 10_000,
      maxQueuedMessages: 1_024,
      ...options,
    };
  }

  get state(): ConnectionState {
    return this.stateValue;
  }

  async connect(): Promise<void> {
    if (this.stateValue === "connected") return;
    if (this.stateValue === "connecting" || this.stateValue === "reconnecting") {
      return new Promise<void>((resolve, reject) => {
        this.connectResolve = resolve;
        this.connectReject = reject;
      });
    }
    this.open(false);
    return new Promise<void>((resolve, reject) => {
      this.connectResolve = resolve;
      this.connectReject = reject;
    });
  }

  disconnect(): void {
    this.options.reconnect = false;
    if (this.reconnectTimer !== undefined) window.clearTimeout(this.reconnectTimer);
    this.reconnectTimer = undefined;
    this.socket?.close(1000, "client disconnect");
    this.socket = undefined;
    this.setState("disconnected");
  }

  publish(subject: string, payload: string | Uint8Array): void {
    const bytes = typeof payload === "string" ? new TextEncoder().encode(payload) : payload;
    const header = new TextEncoder().encode(`PUB ${subject} ${bytes.byteLength}\r\n`);
    this.send(join(header, bytes, CRLF));
  }

  subscribe(subject: string, sid: string, queue?: string): void {
    this.subscriptions.set(sid, { subject, queue });
    this.sendText(queue ? `SUB ${subject} ${queue} ${sid}\r\n` : `SUB ${subject} ${sid}\r\n`);
  }

  unsubscribe(sid: string): void {
    this.subscriptions.delete(sid);
    this.sendText(`UNSUB ${sid}\r\n`);
  }

  ack(ackSubject: string): void {
    this.sendText(`ACK ${ackSubject}\r\n`);
  }

  async request(subject: string, payload: string | Uint8Array, timeoutMs = 30_000): Promise<MorrowMessage> {
    const sid = `request-${crypto.randomUUID()}`;
    const inbox = `_MORROW/INBOX/ws-${crypto.randomUUID()}`;
    this.subscribe(inbox, sid);
    this.publishWithReply(subject, inbox, payload);
    try {
      return await this.nextMessage(timeoutMs, (message) => message.sid === sid);
    } finally {
      this.unsubscribe(sid);
    }
  }

  async nextMessage(timeoutMs = 30_000, predicate: (message: MorrowMessage) => boolean = () => true): Promise<MorrowMessage> {
    const queued = this.messages.findIndex(predicate);
    if (queued >= 0) return this.messages.splice(queued, 1)[0];
    return new Promise<MorrowMessage>((resolve, reject) => {
      const waiter = { predicate, resolve, reject, timer: 0 as unknown as ReturnType<typeof window.setTimeout> };
      waiter.timer = window.setTimeout(() => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) this.waiters.splice(index, 1);
        reject(new Error("timed out waiting for a message"));
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }

  private publishWithReply(subject: string, replyTo: string, payload: string | Uint8Array): void {
    const bytes = typeof payload === "string" ? new TextEncoder().encode(payload) : payload;
    const header = new TextEncoder().encode(`PUB ${subject} ${replyTo} ${bytes.byteLength}\r\n`);
    this.send(join(header, bytes, CRLF));
  }

  private sendText(text: string): void {
    this.send(new TextEncoder().encode(text));
  }

  private send(data: Uint8Array): void {
    if (!this.socket || this.stateValue !== "connected") throw new Error("Morrow WebSocket is not connected");
    this.socket.send(data);
  }

  private open(reconnecting: boolean): void {
    this.setState(reconnecting ? "reconnecting" : "connecting");
    const socket = new WebSocket(this.options.url, "morrow.v1.text");
    socket.binaryType = "arraybuffer";
    this.socket = socket;
    socket.onopen = () => this.emit("open");
    socket.onmessage = (event) => this.receive(event.data);
    socket.onerror = () => this.emit("error");
    socket.onclose = () => {
      this.socket = undefined;
      if (this.stateValue !== "disconnected" && this.options.reconnect !== false) this.scheduleReconnect();
      else this.setState("disconnected");
    };
  }

  private receive(data: string | ArrayBuffer | Blob): void {
    if (typeof data === "string") this.append(new TextEncoder().encode(data));
    else if (data instanceof ArrayBuffer) this.append(new Uint8Array(data));
    else void data.arrayBuffer().then((bytes) => this.append(new Uint8Array(bytes)));
  }

  private append(data: Uint8Array): void {
    this.receiveBuffer = join(this.receiveBuffer, data) as typeof this.receiveBuffer;
    while (true) {
      const lineEnd = findCrlf(this.receiveBuffer);
      if (lineEnd < 0) return;
      const line = new TextDecoder().decode(this.receiveBuffer.slice(0, lineEnd));
      const parts = line.split(" ");
      if (parts[0] === "INFO") {
        void this.authenticate(JSON.parse(line.slice(5)));
        this.receiveBuffer = this.receiveBuffer.slice(lineEnd + 2);
        continue;
      }
      if (parts[0] === "DELIVER" || parts[0] === "HDELIVER") {
        const hasHeaders = parts[0] === "HDELIVER";
        const hasReply = hasHeaders ? parts.length === 6 : parts.length === 5;
        const headersLength = hasHeaders ? Number(parts[hasReply ? 4 : 3]) : 0;
        const size = Number(parts[parts.length - 1]);
        const bodyStart = lineEnd + 2;
        if (this.receiveBuffer.byteLength < bodyStart + size + 2) return;
        const body = this.receiveBuffer.slice(bodyStart, bodyStart + size);
        this.receiveBuffer = this.receiveBuffer.slice(bodyStart + size + 2);
        const headerValues: Record<string, string> = {};
        if (headersLength > 0) {
          for (const header of new TextDecoder().decode(body.slice(0, headersLength)).split("\r\n")) {
            const separator = header.indexOf(": ");
            if (separator > 0) headerValues[header.slice(0, separator)] = header.slice(separator + 2);
          }
        }
        const replyIndex = hasReply ? (hasHeaders ? 3 : 3) : -1;
        const replyTo = replyIndex >= 0 ? parts[replyIndex] : undefined;
        const ackSubject = headerValues["Morrow-Ack"] ?? (replyTo?.startsWith("_MORROW/ACK/") ? replyTo : undefined);
        const message: MorrowMessage = {
          subject: parts[1],
          sid: parts[2],
          replyTo: ackSubject ? undefined : replyTo,
          ackSubject,
          headers: headerValues,
          payload: body.slice(headersLength),
        };
        this.deliver(message);
        continue;
      }
      this.emit("frame", line);
      this.receiveBuffer = this.receiveBuffer.slice(lineEnd + 2);
    }
  }

  private async authenticate(info: { nonce?: string; auth_required?: boolean }): Promise<void> {
    if (info.auth_required && (!this.options.auth || !info.nonce)) {
      this.connectReject?.(new Error("Morrow requires authentication"));
      this.socket?.close(1008, "authentication required");
      return;
    }
    const auth = this.options.auth && info.nonce ? {
      client_id: this.options.auth.clientId,
      signature: await this.options.auth.signNonce(info.nonce),
    } : undefined;
    this.sendText(`CONN ${JSON.stringify({ durable_id: this.options.durableId, ...auth })}\r\n`);
    for (const [sid, subscription] of this.subscriptions) {
      this.sendText(subscription.queue
        ? `SUB ${subscription.subject} ${subscription.queue} ${sid}\r\n`
        : `SUB ${subscription.subject} ${sid}\r\n`);
    }
    this.reconnectAttempt = 0;
    this.setState("connected");
    this.connectResolve?.();
    this.connectResolve = undefined;
    this.connectReject = undefined;
  }

  private deliver(message: MorrowMessage): void {
    const waiterIndex = this.waiters.findIndex((waiter) => waiter.predicate(message));
    if (waiterIndex >= 0) {
      const [waiter] = this.waiters.splice(waiterIndex, 1);
      window.clearTimeout(waiter.timer);
      waiter.resolve(message);
    } else this.enqueue(message);
    this.emit("message", message);
  }

  private enqueue(message: MorrowMessage): void {
    this.messages.push(message);
    if (this.messages.length > this.options.maxQueuedMessages) {
      this.socket?.close(1009, "receive queue limit exceeded");
      this.emit("backpressure");
    }
  }

  private scheduleReconnect(): void {
    this.setState("reconnecting");
    const delay = Math.min(this.options.reconnectMaxDelayMs, this.options.reconnectMinDelayMs * 2 ** this.reconnectAttempt++);
    this.reconnectTimer = window.setTimeout(() => this.open(true), delay);
  }

  private setState(state: ConnectionState): void {
    this.stateValue = state;
    this.dispatchEvent(new CustomEvent("state", { detail: state }));
  }

  private emit(name: string, detail?: unknown): void {
    this.dispatchEvent(new CustomEvent(name, { detail }));
  }
}

const CRLF = new Uint8Array([13, 10]);

function join(...parts: Uint8Array[]): Uint8Array {
  const output = new Uint8Array(parts.reduce((size, part) => size + part.byteLength, 0));
  let offset = 0;
  for (const part of parts) {
    output.set(part, offset);
    offset += part.byteLength;
  }
  return output;
}

function findCrlf(bytes: Uint8Array): number {
  for (let index = 0; index + 1 < bytes.length; index++) {
    if (bytes[index] === 13 && bytes[index + 1] === 10) return index;
  }
  return -1;
}
