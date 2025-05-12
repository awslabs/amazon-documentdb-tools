import * as crypto from "node:crypto"

export class AppConfig {
  public PORT: number
  public MAIN_API_KEY: string
  public MONGODB_CONNECTION_STRING_KEY: string
}

function getRandomKey(): string {
  return crypto
    .createHash("sha256")
    .update(Math.random().toString())
    .digest("hex")
}

let config: AppConfig | undefined = undefined
export function getAppConfig(): AppConfig {
  if (config) {
    return config
  }
  config = new AppConfig()
  config.PORT = parseInt(process.env.PORT) || 3000
  config.MAIN_API_KEY = process.env.MAIN_API_KEY || getRandomKey()
  config.MONGODB_CONNECTION_STRING_KEY =
    process.env.MONGODB_CONNECTION_STRING_KEY ||
    "mongodb://localhost:27070/data-api"
  return config
}
