import { MongoClient } from "mongodb"
import { Inject, Injectable } from "@nestjs/common"
import { AppConfig } from "./app-config"
import { getParameter } from "./aws-utils"

@Injectable()
export class MongoDBProvider {
  mongoClient: MongoClient

  constructor(@Inject("AppConfig") appConfig: AppConfig) {
    let connectionStringKey = appConfig.MONGODB_CONNECTION_STRING_KEY
    if (!connectionStringKey) {
      throw new Error("No connection string key provided")
    }
    if (!connectionStringKey.startsWith("mongodb://")) {
      let connectionString = getParameter(connectionStringKey)
      if (!connectionString) {
        throw new Error("No connection string provided")
      }
    }
    this.mongoClient = new MongoClient(connectionStringKey)
    this.mongoClient.connect().then(() => {})
  }
}
