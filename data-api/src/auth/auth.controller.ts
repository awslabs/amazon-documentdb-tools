import { Controller, Post, Body, Headers } from "@nestjs/common"
import { AuthService } from "./auth.service"
import { ApiKeyAuthDto } from "./dto/apikey-auth.dto"
import { AuthResponseDto, SessionResponseDto } from "./dto/auth.response.dto"
import { getBearerToken } from "../utils/http-headers"

@Controller("auth")
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  // @Post("providers/api-key/login")
  // loginWithApiKey(@Body() apiKeyAuthDto: ApiKeyAuthDto): AuthResponseDto {
  //   return this.authService.loginWithApiKey(apiKeyAuthDto)
  // }
  //
  // @Post("session")
  // session(@Headers() headers: Record<string, string>): SessionResponseDto {
  //   let refresh_token = getBearerToken(headers)
  //   return this.authService.session(refresh_token)
  // }
}
