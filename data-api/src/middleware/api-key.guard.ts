import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Inject,
  Injectable,
  UnauthorizedException,
} from "@nestjs/common"
import { ActionService } from "../action/action.service"
import { AppConfig } from "../utils/app-config"
import { MongoDBProvider } from "../utils/mongodb-provider"
import { createHash } from "crypto"

@Injectable()
export class ApiKeyGuard implements CanActivate {
  mongodbProvider: MongoDBProvider

  constructor(@Inject() mongodbProvider: MongoDBProvider) {
    this.mongodbProvider = mongodbProvider
  }

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest()
    const keyName: string = "apiKey" in request.headers ? "apiKey" : "apikey"
    const apiKey = request.headers[keyName] ?? request.query.apiKey // checks the header, moves to query if null
    if (!apiKey) {
      throw new UnauthorizedException("API key is required")
    }
    try {
      let doc = await this.validateKeyAndGetRole(apiKey)
      request.user = {
        id: doc["username"],
        type: doc["type"],
        roles: [doc["role"]],
      }
    } catch (e) {
      throw new ForbiddenException("Invalid API")
    }
    return true
  }

  async validateKeyAndGetRole(apiKey: string): Promise<any> {
    if (apiKey.length !== 64) {
      throw new ForbiddenException("Invalid API key")
    }
    const hashedKey = createHash("sha256").update(apiKey).digest("hex")
    const doc = await this.mongodbProvider.mongoClient
      .db("aws_apps")
      .collection("users")
      .findOne({ hashed_key: hashedKey })
    if (!doc) {
      throw new ForbiddenException("Invalid API key")
    }
    return doc
  }
}
