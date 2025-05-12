import { NestFactory } from "@nestjs/core"
import { join, resolve } from "path"
import {
  FastifyAdapter,
  NestFastifyApplication,
} from "@nestjs/platform-fastify"
import { AppModule } from "./app.module"
import { ValidationPipe } from "@nestjs/common"
import { Logger } from "nestjs-pino"
import { configureDocumentation } from "./utils/create-docs"
import { getAppConfig } from "./utils/app-config"
import { getExtendedJsonContentTypeParser } from "./middleware/extended-json.body-parser"

async function bootstrap() {
  let adapter = new FastifyAdapter({ logger: true })
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    adapter,
    { bufferLogs: true },
  )
  adapter
    .getInstance()
    .addContentTypeParser(
      "application/ejson",
      { bodyLimit: 16 * 1024 * 1024 },
      getExtendedJsonContentTypeParser,
    )
  adapter.getInstance().register(require("@fastify/static"), {
    root: resolve(join(__dirname, "../public")),
    prefix: "/public/",
  })

  let logger = app.get(Logger)
  app.useLogger(logger)
  app.useGlobalPipes(new ValidationPipe())
  configureDocumentation(app)

  let appConfig = getAppConfig()
  logger.log("Starting the data-api on port: " + appConfig.PORT)
  await app.listen(appConfig.PORT, "0.0.0.0")
  logger.log("Your main API key is: " + appConfig.MAIN_API_KEY)
}

bootstrap().then(() => {})
