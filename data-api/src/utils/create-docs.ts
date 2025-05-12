import { INestApplication } from "@nestjs/common"
import { DocumentBuilder, SwaggerModule } from "@nestjs/swagger"
import { RapidocModule } from "@b8n/nestjs-rapidoc"

export function configureDocumentation(app: INestApplication<any>) {
  let description: string =
    "Amazon DocumentDB Data API allow your client applications and services access Amazon DocumentDB " +
    "databases using a simple and secure REST API service from outside of the Amazon Virtual Private Cloud " +
    "(VPC), providing more flexibility and easier integrations with your existing applications."
  const config = new DocumentBuilder()
    .setTitle("Amazon DocumentDB Data API")
    .setDescription(description)
    .setVersion("1.0")
    .addTag("data-api")
    .addApiKey({ type: "apiKey", in: "header", name: "apiKey" }, "apiKey")
    .build()
  const document = SwaggerModule.createDocument(app, config)
  const options = {
    customLogo: "./public/assets/documentdb.svg",
    customFavIcon: "./public/assets/favicon.ico",
    customSiteTitle: "Amazon DocumentDB Data API",
  }
  RapidocModule.setup("api", app, document, options)
}
