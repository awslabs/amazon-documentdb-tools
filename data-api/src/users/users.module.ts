import { Module } from "@nestjs/common"
import { UsersService } from "./users.service"
import { UsersController } from "./users.controller"
import { MongoDBProvider } from "../utils/mongodb-provider"
import { ActionModule } from "../action/action.module"

@Module({
  controllers: [UsersController],
  providers: [UsersService],
  imports: [ActionModule],
})
export class UsersModule {}
