import { Config } from "../config"
import { APIRequestContext } from "@playwright/test"
import { expect } from "./baseFixture"

export class OauthRequestContext {
  readonly baseUrl: string
  readonly request: APIRequestContext

  constructor(config: Config, request: APIRequestContext) {
    this.baseUrl = config.baseUrl
    this.request = request
  }

  async getAccessToken(clientId: string): Promise<string> {
    const response = await this.request.post(this.baseUrl + "dev/oauth/token", {
      form: {
        grant_type: "client_credentials",
        client_id: clientId,
        client_secret: "test-secret-key-which-is-long-enough",
      },
    })

    expect(response.status()).toBe(200)
    const body = await response.json()
    return body.access_token
  }

  async getAuthorizationHeader(
    clientId: string,
  ): Promise<{ Authorization: string }> {
    return { Authorization: `Bearer ${await this.getAccessToken(clientId)}` }
  }
}
