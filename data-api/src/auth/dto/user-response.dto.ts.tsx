export class UserDataDto {
  name: string
}
export class IdentityResponseDto {
  id: number
  provider_type: string
  data: UserDataDto
}
export class UserResponseDto {
  id: number
  type: string
  data: UserDataDto
  identities: IdentityResponseDto[]
}
