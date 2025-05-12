import { Injectable } from "@nestjs/common"
import { ApiKeyAuthDto } from "./dto/apikey-auth.dto"
import { AuthResponseDto, SessionResponseDto } from "./dto/auth.response.dto"

@Injectable()
export class AuthService {
  loginWithApiKey(dto: ApiKeyAuthDto): AuthResponseDto {
    throw new Error("Method not implemented.")
    // use dto.key to validate if the user is valid and active
    // if user is valid, create a new session and return it
    // you might have to store the session in a users table
  }

  session(refresh_token: string): SessionResponseDto {
    throw new Error("Method not implemented.")
    // use dto.key to validate if the user is valid and active
    // if user is valid, create a new session and return it
    // you might have to store the session in a users table
  }
}
