import {
  Inject,
  Injectable,
  Logger,
  NotImplementedException,
  OnModuleInit,
} from "@nestjs/common"
import { MongoDBProvider } from "../utils/mongodb-provider"
import { AppConfig } from "../utils/app-config"
import { createHash } from "crypto"

const logger = new Logger("actions.service")

@Injectable()
export class DbInitializer implements OnModuleInit {
  mongodbProvider: MongoDBProvider
  appConfig: AppConfig
  constructor(
    @Inject("AppConfig") appConfig: AppConfig,
    @Inject(MongoDBProvider) mongodbProvider: MongoDBProvider,
  ) {
    this.mongodbProvider = mongodbProvider
    this.appConfig = appConfig
  }

  async onModuleInit(): Promise<void> {
    try {
      logger.log(`Checking for master apiKey on aws_apps.users collection`)
      const keyName: string = "master-api-key"
      let apiKeyDoc = await this.getApiKeyByName(keyName)
      if (!apiKeyDoc) {
        logger.log(`No master apiKey exists on aws_apps.users collection`)
        await this.insertApiKey(keyName, this.appConfig.MAIN_API_KEY)
        logger.log(`Completed creating new apiKey on aws_apps.users collection`)
      } else {
        logger.log(`Master apiKey already exists. Setting the environment key`)
        this.appConfig.MAIN_API_KEY = apiKeyDoc["key"]
      }
    } catch (error) {
      logger.error(`Error connecting to MongoDB: ${error}`)
    }
  }

  async getApiKeyByName(keyName: string): Promise<any> {
    try {
      return await this.mongodbProvider.mongoClient
        .db("aws_apps")
        .collection("users")
        .findOne({ username: keyName, type: "api-key" })
    } catch (error) {
      return null
    }
  }

  async insertApiKey(keyName: string, apiKey: string): Promise<any> {
    try {
      const hashedKey = createHash("sha256").update(apiKey).digest("hex")
      const maskedKey =
        apiKey.substring(0, 2) + "......." + apiKey.substring(apiKey.length - 4)
      return await this.mongodbProvider.mongoClient
        .db("aws_apps")
        .collection("users")
        .insertOne({
          username: keyName,
          type: "api-key",
          role: "admin",
          key: apiKey, // well do not store the plain text key
          hashed_key: hashedKey,
          masked_key: maskedKey,
          created_at: new Date(),
          expires_at: new Date("9999-12-31"),
        })
    } catch (error) {
      throw new NotImplementedException(`Error inserting API key: ${error}`)
    }
  }
}
