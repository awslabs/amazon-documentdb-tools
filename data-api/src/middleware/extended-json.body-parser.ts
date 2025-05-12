import { EJSON } from "bson"

export function getExtendedJsonContentTypeParser(
  request: any,
  response: any,
): Promise<string | Error> {
  return new Promise<string>((resolve, reject) => {
    let rawBody = ""
    request.raw.on("data", (chunk) => {
      rawBody += chunk
    })
    request.raw.on("end", () => {
      resolve(EJSON.parse(rawBody))
    })
    request.raw.on("error", (err) => {
      response.statusCode = 500
      response.end("Error reading the request body")
      reject(new Error("Error reading the request body"))
    })
  })
}
