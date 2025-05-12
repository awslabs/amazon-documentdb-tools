export class User {
  id: number
  username: string
  type: "api-key"
  role: "admin"
  key: string
  masked_key: string
  expires_at: Date
  created_at: Date
}
