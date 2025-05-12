import { Module } from "@nestjs/common"
import { ActionService } from "./action.service"
import { ActionController } from "./action.controller"
import { MongoDBProvider } from "../utils/mongodb-provider"
import { appConfigProvider } from "../utils/app-config.provider"

@Module({
  controllers: [ActionController],
  providers: [ActionService, appConfigProvider, MongoDBProvider],
  exports: [MongoDBProvider],
})
export class ActionModule {}
