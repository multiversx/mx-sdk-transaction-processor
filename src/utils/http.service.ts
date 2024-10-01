import axios from "axios";

export class HttpService {
  private readonly DEFAULT_TIMEOUT = 5000;
  private readonly baseUrl: string | undefined;
  private readonly timeout: number;

  constructor(
    baseUrl: string | undefined,
    timeout: number | undefined = undefined,
  ) {
    this.baseUrl = baseUrl;
    this.timeout = timeout ?? this.DEFAULT_TIMEOUT;
  }

  async get<T = any>(
    path: string,
  ): Promise<T> {
    const gatewayUrl = this.baseUrl ?? 'https://gateway.multiversx.com';
    const fullUrl = `${gatewayUrl}/${path}`;

    try {
      const result = await axios.get(fullUrl, {
        timeout: this.timeout,
      });
      return result.data.data;
    } catch (error) {
      throw new Error(`Error when getting from url ${fullUrl}: ${error}`);
    }
  }
}
