import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Injectable,
  UnauthorizedException,
} from "@nestjs/common"
import { Reflector } from "@nestjs/core"

export enum UserRole {
  Admin = "admin",
  ReadOnly = "read",
  WriteOnly = "write",
  ReadWrite = "readWrite",
}
import { SetMetadata } from "@nestjs/common"

export const ROLES_KEY = "roles"
export const Roles = (...roles: UserRole[]) => SetMetadata(ROLES_KEY, roles)

@Injectable()
export class UserRoleGuard implements CanActivate {
  constructor(private reflector: Reflector) {
    this.reflector = reflector
  }

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const requiredRoles = this.reflector.getAllAndOverride<UserRole[]>(
      ROLES_KEY,
      [context.getHandler(), context.getClass()],
    )
    if (!requiredRoles) {
      return true
    }
    const { user } = context.switchToHttp().getRequest()
    const hasRole = requiredRoles.some((role) => user.roles?.includes(role))
    if (!hasRole) {
      throw new ForbiddenException(
        "You do not have permission to access this resource",
      )
    }
    return true
  }

  getApiKeyFromRequest(request: any): string {
    const keyName: string = "apiKey" in request.headers ? "apiKey" : "apikey"
    const key = request.headers[keyName] ?? request.query.apiKey // checks the header, moves to query if null
    if (!key) {
      throw new UnauthorizedException("API key is required")
    }
    return key
  }
}
