export class SessionResponseDto {
  access_token: string
}

export class AuthResponseDto extends SessionResponseDto {
  refresh_token: string
  user_id: string
}
