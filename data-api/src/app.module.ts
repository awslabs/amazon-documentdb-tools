import { MiddlewareConsumer, Module, NestModule } from "@nestjs/common"
import { LoggerModule } from "nestjs-pino"
import { ActionModule } from "./action/action.module"
import { AuthModule } from "./auth/auth.module"
import { DbInitializer } from "./initializers/db-initializer"
import { appConfigProvider } from "./utils/app-config.provider"
import { UsersModule } from './users/users.module';

@Module({
  imports: [LoggerModule.forRoot(), ActionModule, AuthModule, UsersModule],
  providers: [appConfigProvider, DbInitializer],
  controllers: [],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer) {}
}
