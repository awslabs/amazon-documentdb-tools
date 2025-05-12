import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm"
import { Logger } from "@nestjs/common"

const client = new SSMClient({ region: "us-east-1" })
const logger = new Logger("aws-utils")

export async function getParameter(name: string): Promise<string | undefined> {
  let command = new GetParameterCommand({
    Name: name,
    WithDecryption: true,
  })
  try {
    let response = await client.send(command)
    const value = response.Parameter?.Value || ""
    if (value.indexOf(":") > -1) {
      // value has a pas
      let displayValue = value.replace(/\/\/([^:]+):(.*)@/, "//$1:***@")
      logger.log("Parameter value: ", displayValue)
    }
    return value
  } catch (err) {
    logger.error(`Error while fetching the SSM parameter: ${name}`)
    logger.error(err)
    return undefined
  }
}
