import axios, { AxiosInstance } from "axios";
import * as http from "http";
import * as https from "https";
import { DEFAULT_GATEWAY_URL, DEFAULT_TIMEOUT, MAX_SOCKETS_PER_HOST } from "./constants";

// Deliberately shared by every HttpService instance rather than owned per instance.
// TransactionProcessor.start() is normally driven by a sub-second cron and rebuilds its
// HttpService on each tick; per-instance agents would discard the socket pool every tick and
// every request would pay a fresh TLS handshake again (~165ms vs ~52ms against the public
// gateway). Node 19+ defaults its global agent to keepAlive, this keeps the behaviour on
// older runtimes too.
const keepAliveHttpAgent = new http.Agent({ keepAlive: true, maxSockets: MAX_SOCKETS_PER_HOST });
const keepAliveHttpsAgent = new https.Agent({ keepAlive: true, maxSockets: MAX_SOCKETS_PER_HOST });

export class HttpService {
  private readonly baseUrl: string;
  private readonly client: AxiosInstance;

  constructor(
    baseUrl: string | undefined,
    timeout: number | undefined = undefined,
  ) {
    this.baseUrl = baseUrl ?? DEFAULT_GATEWAY_URL;
    this.client = axios.create({
      baseURL: this.baseUrl,
      timeout: timeout ?? DEFAULT_TIMEOUT,
      httpAgent: keepAliveHttpAgent,
      httpsAgent: keepAliveHttpsAgent,
    });
  }

  async get<T = any>(
    path: string,
  ): Promise<T> {
    try {
      const result = await this.client.get(path);
      return result.data.data;
    } catch (error) {
      throw new Error(`Error when getting from url ${this.baseUrl}/${path}: ${error}`);
    }
  }
}
