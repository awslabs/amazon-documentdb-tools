export function getBearerToken(headers: Record<string, string>): string {
  let key: string = "Authorization"
  let value = headers[key]
  if (!value) {
    throw new Error(`${key} header is required`)
  }
  let parts = value.split(" ")
  if (parts.length !== 2) {
    throw new Error(`${key} header is invalid`)
  }
  if (parts[0] !== "Bearer") {
    throw new Error(`${key} header is invalid`)
  }
  return parts[1]
}
