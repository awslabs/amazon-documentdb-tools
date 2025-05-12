import { Provider } from "@nestjs/common"
import { getAppConfig } from "./app-config"

export const appConfigProvider: Provider = {
  provide: "AppConfig",
  useValue: getAppConfig(),
}
