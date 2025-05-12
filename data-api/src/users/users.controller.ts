import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
  UseGuards,
} from "@nestjs/common"
import { UsersService } from "./users.service"
import { CreateUserDto } from "./dto/create-user.dto"
import { UpdateUserDto } from "./dto/update-user.dto"
import { ApiKeyGuard } from "../middleware/api-key.guard"
import { Roles, UserRole, UserRoleGuard } from "../middleware/user-role.guard"
import {
  ApiBadRequestResponse,
  ApiCreatedResponse,
  ApiForbiddenResponse,
  ApiOkResponse,
  ApiOperation,
  ApiSecurity,
  ApiTags,
  ApiUnauthorizedResponse,
} from "@nestjs/swagger"
import { InsertOneResponseBody } from "../action/dto/insert-one-response-body"
import { ErrorMessages } from "../utils/messages"
import { ErrorResponse } from "../action/dto/error-response"
import { CreateUserResponseDto } from "./dto/create-user.response.dto"
import { FindUsersResponseBody } from "./find-users.response-body"

@ApiTags("Administration")
@UseGuards(ApiKeyGuard, UserRoleGuard) // ,
@Controller("users")
export class UsersController {
  constructor(private readonly usersService: UsersService) {}

  @Roles(UserRole.Admin)
  @ApiSecurity("apiKey")
  @Post()
  @ApiOperation({
    summary: "Create an apiKey user",
    description: "Create a new user with an apiKey",
  })
  @ApiCreatedResponse({
    description: "The result of a create apiKey user operation.",
    type: CreateUserResponseDto,
  })
  @ApiBadRequestResponse({
    description: ErrorMessages.INVALID_REQUEST,
    type: ErrorResponse,
  })
  @ApiUnauthorizedResponse({
    description: ErrorMessages.UNAUTHORIZED_REQUEST,
    type: ErrorResponse,
  })
  @ApiForbiddenResponse({
    description: ErrorMessages.FORBIDDEN_REQUEST,
    type: ErrorResponse,
  })
  create(@Body() createUserDto: CreateUserDto) {
    return this.usersService.create(createUserDto)
  }

  @Roles(UserRole.Admin)
  @ApiSecurity("apiKey")
  @Get()
  @ApiOperation({
    summary: "List apiKey users",
    description: "List all users with an apiKey",
  })
  @ApiOkResponse({
    description: "The result of a list apiKey users operation.",
    type: FindUsersResponseBody,
  })
  @ApiBadRequestResponse({
    description: ErrorMessages.INVALID_REQUEST,
    type: ErrorResponse,
  })
  @ApiUnauthorizedResponse({
    description: ErrorMessages.UNAUTHORIZED_REQUEST,
    type: ErrorResponse,
  })
  @ApiForbiddenResponse({
    description: ErrorMessages.FORBIDDEN_REQUEST,
    type: ErrorResponse,
  })
  async findAll() {
    let users = await this.usersService.findAll()
    // return users
    return {
      users: users,
    }
  }
  //
  // @Get(':id')
  // findOne(@Param('id') id: string) {
  //   return this.usersService.findOne(+id);
  // }
  //
  // @Patch(':id')
  // update(@Param('id') id: string, @Body() updateUserDto: UpdateUserDto) {
  //   return this.usersService.update(+id, updateUserDto);
  // }
  //
  // @Delete(':id')
  // remove(@Param('id') id: string) {
  //   return this.usersService.remove(+id);
  // }
}
